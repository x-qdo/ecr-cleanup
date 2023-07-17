'''
Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
'''

import argparse
import os
import re
from datetime import timedelta, datetime

import boto3
from kubernetes import config, client

REGION = None
DRYRUN = None
IMAGES_TO_KEEP = 300
IMAGES_KEEP_DURATION = "3m"
IGNORE_TAGS_REGEX = None
IGNORE_REPO_REGEX = None
CLUSTERS = []


def initialize():
    global REGION
    global DRYRUN
    global IMAGES_TO_KEEP
    global IGNORE_TAGS_REGEX
    global IGNORE_REPO_REGEX
    global CLUSTERS
    global IMAGES_KEEP_DURATION

    REGION = os.environ.get('REGION', "None")
    DRYRUN = os.environ.get('DRYRUN', "false").lower()
    IMAGES_KEEP_DURATION = os.environ.get('IMAGES_KEEP_DURATION', "3m")
    CLUSTERS = os.environ.get('CLUSTERS', "None").split(",")
    if DRYRUN == "false":
        DRYRUN = False
    else:
        DRYRUN = True
    IMAGES_TO_KEEP = int(os.environ.get('IMAGES_TO_KEEP', 100))
    IGNORE_TAGS_REGEX = os.environ.get('IGNORE_TAGS_REGEX', "^$")
    IGNORE_REPO_REGEX = os.environ.get('IGNORE_REPO_REGEX', "^$")


def handler(event, context):
    initialize()
    if REGION == "None":
        ec2_client = boto3.client('ec2')
        available_regions = ec2_client.describe_regions()['Regions']
        for region in available_regions:
            discover_delete_images(region['RegionName'], CLUSTERS)
    else:
        discover_delete_images(REGION, CLUSTERS)


def convert_duration_to_timedelta(duration_str):
    number = int(re.search(r'\d+', duration_str).group())
    if 'd' in duration_str:
        return timedelta(days=number)
    elif 'w' in duration_str:
        return timedelta(weeks=number)
    elif 'm' in duration_str:
        return timedelta(days=number * 30)
    else:
        raise ValueError(f'Invalid duration string: {duration_str}')


def get_eks_pods_images(cluster):
    config.load_kube_config(context=cluster)
    v1 = client.CoreV1Api()
    pods = v1.list_pod_for_all_namespaces().items
    running_images = []
    for pod in pods:
        for container in pod.spec.containers:
            if container.image not in running_images:
                running_images.append(container.image)
    return running_images


def discover_delete_images(region_name, clusters):
    global time_limit
    print("Discovering images in " + region_name)
    ecr_client = boto3.client('ecr', region_name=region_name)

    repositories = []
    describe_repo_paginator = ecr_client.get_paginator('describe_repositories')
    for response_listrepopaginator in describe_repo_paginator.paginate():
        for repo in response_listrepopaginator['repositories']:
            repositories.append(repo)

    running_containers = []
    for cluster in clusters:
        print(f'Discovering running pods in {cluster}')
        running_containers.extend(get_eks_pods_images(cluster))

    print("Images that are running:")
    for image in running_containers:
        print(image)

    ignore_repo_regex = re.compile(IGNORE_REPO_REGEX)

    for repository in repositories:
        if re.search(ignore_repo_regex, repository['repositoryUri']):
            print("------------------------")
            print("Skipping repository: " + repository['repositoryUri'])
            continue

        print("------------------------")
        print("Starting with repository: " + repository['repositoryUri'])
        deletesha = []
        deletetag = []
        tagged_images = []


        describe_image_paginator = ecr_client.get_paginator('describe_images')
        for response_describe_image_paginator in describe_image_paginator.paginate(
                registryId=repository['registryId'],
                repositoryName=repository['repositoryName']):
            for image in response_describe_image_paginator['imageDetails']:
                if 'imageTags' in image:
                    tagged_images.append(image)
                else:
                    append_to_list(deletesha, image['imageDigest'])

        print("Total number of images found: {}".format(len(tagged_images) + len(deletesha)))
        print("Number of untagged images found {}".format(len(deletesha)))

        tagged_images.sort(key=lambda k: k['imagePushedAt'], reverse=True)

        # Get ImageDigest from ImageURL for running images. Do this for every repository
        # Converting running_containers to set for efficient lookup
        running_containers_set = set(running_containers)

        running_sha = []

        # Calculate abs point to check how old images are
        keep_duration = convert_duration_to_timedelta(IMAGES_KEEP_DURATION)

        for image in tagged_images:
            time_limit = datetime.now(tz=image['imagePushedAt'].tzinfo) - keep_duration
            for tag in image['imageTags']:
                imageurl = repository['repositoryUri'] + ":" + tag
                if imageurl in running_containers_set:  # This is a constant time operation
                    running_sha.append(image['imageDigest'])

        # Remove duplicates in running_sha
        running_sha_set = set(running_sha)

        print("Number of running images found {}".format(len(running_sha)))
        ignore_tags_regex = re.compile(IGNORE_TAGS_REGEX)

        for index, image in enumerate(tagged_images):
            if image['imagePushedAt'] < time_limit or index >= IMAGES_TO_KEEP:
                for tag in image['imageTags']:
                    if "latest" not in tag and ignore_tags_regex.search(tag) is None:
                        # Add a condition to exclude repo that contain the name regex
                        if not running_sha_set or image['imageDigest'] not in running_sha_set:
                            append_to_list(deletesha, image['imageDigest'])
                            append_to_tag_list(deletetag, {"imageUrl": repository['repositoryUri'] + ":" + tag,
                                                           "pushedAt": image["imagePushedAt"]})

        if deletesha:
            print("Number of images to be deleted: {}".format(len(deletesha)))
            delete_images(
                ecr_client,
                deletesha,
                deletetag,
                repository['registryId'],
                repository['repositoryName']
            )
        else:
            print("Nothing to delete in repository : " + repository['repositoryName'])


def append_to_list(image_digest_list, repo_id):
    if not {'imageDigest': repo_id} in image_digest_list:
        image_digest_list.append({'imageDigest': repo_id})


def append_to_tag_list(tag_list, tag_id):
    if not tag_id in tag_list:
        tag_list.append(tag_id)


def chunks(repo_list, chunk_size):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(repo_list), chunk_size):
        yield repo_list[i:i + chunk_size]


def delete_images(ecr_client, deletesha, deletetag, repo_id, name):
    if len(deletesha) >= 1:
        ## spliting list of images to delete on chunks with 100 images each
        ## http://docs.aws.amazon.com/AmazonECR/latest/APIReference/API_BatchDeleteImage.html#API_BatchDeleteImage_RequestSyntax
        i = 0
        for deletesha_chunk in chunks(deletesha, 100):
            i += 1
            if not DRYRUN:
                delete_response = ecr_client.batch_delete_image(
                    registryId=repo_id,
                    repositoryName=name,
                    imageIds=deletesha_chunk
                )
                print(delete_response)
            else:
                print("registryId:" + repo_id)
                print("repositoryName:" + name)
                print("Deleting {} chank of images".format(i))
                print("imageIds:", end='')
                print(deletesha_chunk)
    if deletetag:
        print("Image URLs that are marked for deletion:")
        for ids in deletetag:
            print("- {} - {}".format(ids["imageUrl"], ids["pushedAt"]))


# Below is the test harness
if __name__ == '__main__':
    REQUEST = {"None": "None"}
    PARSER = argparse.ArgumentParser(description='Deletes stale ECR images')
    PARSER.add_argument('--dryrun', help='Prints the repository to be deleted without deleting them', default='true',
                        action='store', dest='dryrun')
    PARSER.add_argument('-i', '--imagestokeep', help='Number of image tags to keep', default='100', action='store',
                        dest='imagestokeep')
    PARSER.add_argument('-d', '--imagestokeepduration', help='Duration from last push', default='3m', action='store',
                        dest='imagestokeepduration')
    PARSER.add_argument('-r', '--region', help='ECR/ECS region', action='store', dest='region', required=True)
    PARSER.add_argument('-re', '--ignoretagsregex', help='Regex of tag names to ignore', default="^$", action='store',
                        dest='ignoretagsregex')
    PARSER.add_argument('-c', '--cluster', nargs='+', help='<Required> Add context to parse', required=True)
    PARSER.add_argument('-ir', '--ignorereporegex', help='Regex of repo names to ignore', default="^$", action='store',
                            dest='ignorereporegex')

    ARGS = PARSER.parse_args()
    if ARGS.region:
        os.environ["REGION"] = ARGS.region
    else:
        os.environ["REGION"] = "None"
    os.environ["DRYRUN"] = ARGS.dryrun.lower()
    os.environ["IMAGES_TO_KEEP"] = ARGS.imagestokeep
    os.environ["IGNORE_TAGS_REGEX"] = ARGS.ignoretagsregex
    os.environ["IGNORE_REPO_REGEX"] = ARGS.ignorereporegex
    os.environ["IMAGES_KEEP_DURATION"] = ARGS.imagestokeepduration
    os.environ["CLUSTERS"] = ','.join(ARGS.cluster)
    handler(REQUEST, None)
