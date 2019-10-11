from os import path
import yaml
from kubernetes import client, config, utils, watch
import json
from os import system
from zun_ui.api import cfy
from kubernetes.client.rest import ApiException
from pprint import pprint
from django.conf import settings

def load_k8s_config():
    # load  k8s-config
    k8s_configfile_path = getattr(settings, "K8S_CONFIG_FILE_PATH", None)
    if k8s_configfile_path is None:
        config.load_kube_config("//.kube/config")
    else:
        config.load_kube_config(k8s_configfile_path)

def list_all_pods():

    load_k8s_config()
    v1 = client.CoreV1Api()
    result_set = v1.list_pod_for_all_namespaces(watch = False)

    pods_infos = []
    for item in result_set.items:
        pod_info = {}
        pod_info['pods_name'] = item.metadata.name
        pod_info['pods_namespace'] = item.metadata.namespace
        pod_info['pods_labels'] = item.metadata.labels
        pod_info['id'] = item.metadata.uid
        pods_infos.append(pod_info)
    # print json.dumps(pods_infos)
    pods_infos_to_dict = {}
    pods_infos_to_dict['pods_info'] = pods_infos
    return json.dumps(pods_infos_to_dict)

def list_all_pods2(who, who_id):

    load_k8s_config()
    v1 = client.CoreV1Api()
    result_set = v1.list_pod_for_all_namespaces(watch = False)

    pods_infos = []

    if str(who) == 'admin':
        for item in result_set.items:
            pod_info = {}
            pod_info['pods_name'] = item.metadata.name
            pod_info['pods_namespace'] = item.metadata.namespace
            pod_info['pods_labels'] = item.metadata.labels
            pod_info['id'] = item.metadata.uid
            pods_infos.append(pod_info)
    else:
        for item in result_set.items:
            if item.metadata.namespace == str(who):
                pod_info = {}
                pod_info['pods_name'] = item.metadata.name
                pod_info['pods_namespace'] = item.metadata.namespace
                pod_info['pods_labels'] = item.metadata.labels
                pod_info['id'] = item.metadata.uid
                pods_infos.append(pod_info)
    # print json.dumps(pods_infos)
    pods_infos_to_dict = {}
    pods_infos_to_dict['pods_info'] = pods_infos
    return json.dumps(pods_infos_to_dict)


def create_deployment_from_yaml(json_file):

    load_k8s_config()
    k8s_client = client.ApiClient()
    # print "k8s_client:", json_file
    cfy.create_from_yaml_single_item(k8s_client, json_file)


def list_all_deployment():

    load_k8s_config()
    # create an instance of the API class
    api_response = client.AppsV1Api()
    try:
        deployment_result_set = api_response.list_deployment_for_all_namespaces()
        # print deployment_result_set
        deployment_info = []
        for item in deployment_result_set.items:
            deployment_item = {}
            deployment_item['uuid'] = item.metadata.uid
            deployment_item['deployment_name'] = item.metadata.name
            deployment_item['deployment_namespace'] = item.metadata.namespace
            deployment_item['deployment_labels'] = item.metadata.labels
            deployment_info.append(deployment_item)

        deployment_info_to_dict = {}
        deployment_info_to_dict['deployments_info'] = deployment_info
        # print json.dumps(deployment_info_to_dict)
    except Exception as e:
        print "list_all_deployment() exception !!!"
        print e
    # print json.dumps(deployment_info_to_dict)
    return json.dumps(deployment_info_to_dict)

def list_all_deployment2(who, who_id):

    load_k8s_config()
    # create an instance of the API class
    api_response = client.AppsV1Api()
    try:
        deployment_result_set = api_response.list_deployment_for_all_namespaces()
        deployment_info = []
        if str(who) == 'admin':
            for item in deployment_result_set.items:
                deployment_item = {}
                deployment_item['uuid'] = item.metadata.uid
                deployment_item['deployment_name'] = item.metadata.name
                deployment_item['deployment_namespace'] = item.metadata.namespace
                deployment_item['deployment_labels'] = item.metadata.labels
                deployment_info.append(deployment_item)
        else:
            for item in deployment_result_set.items:
                if item.metadata.namespace == str(who):
                    deployment_item = {}
                    deployment_item['uuid'] = item.metadata.uid
                    deployment_item['deployment_name'] = item.metadata.name
                    deployment_item['deployment_namespace'] = item.metadata.namespace
                    deployment_item['deployment_labels'] = item.metadata.labels
                    deployment_info.append(deployment_item)

        deployment_info_to_dict = {}
        deployment_info_to_dict['deployments_info'] = deployment_info
        # print json.dumps(deployment_info_to_dict)
    except Exception as e:
        print "exception info: "
        print e
    print json.dumps(deployment_info_to_dict)
    return json.dumps(deployment_info_to_dict)


def get_deployment_info_from_id(id):

    load_k8s_config()
    api_instance = client.AppsV1Api()

    deployment_result_set = api_instance.list_deployment_for_all_namespaces()
    id_to_deployment_info = "{ \"id\":" + json.dumps(id) + ","
    id_to_deployment_info = id_to_deployment_info + "\"id_deployment_info\": ["
    str_id = str(id)

    for item_deployment in deployment_result_set.items:

        if item_deployment.metadata.uid == str_id:

            metadata = item_deployment.metadata.to_dict()
            for metadata_key, metadata_value in metadata.items():
                try:
                    json.dumps(metadata_value)
                except TypeError:
                    key = metadata_key
                    value = metadata_value
                    del metadata[key]
                    metadata[key] = str(value)

            id_to_deployment_info = id_to_deployment_info + "{ \"metadata\": " + json.dumps(metadata) + "},"

            spec = item_deployment.spec.to_dict()
            for spec_key, spec_value in spec.items():
                if spec_key == 'template':
                    del spec[spec_key]
                    continue
                try:
                    json.dumps(spec_value)
                except TypeError:
                    key = spec_key
                    value = spec_value
                    del spec[key]
                    spec[key] = str(value)

            id_to_deployment_info = id_to_deployment_info + "{ \"spec\": " + json.dumps(spec) + "},"

            status = item_deployment.status.to_dict()
            for status_key, status_value in status.items():
                if status_key == 'conditions':
                    del status[status_key]
                    continue
                try:
                    json.dumps(status_value)
                except TypeError:
                    key = status_key
                    value = status_value
                    del status[key]
                    status[key] = str(value)

            id_to_deployment_info = id_to_deployment_info + " { \"status\": " + json.dumps(status) + "}]}"
            break

    # print id_to_deployment_info
    return id_to_deployment_info

def get_deployment_info_from_id2(id):

    load_k8s_config()
    api_instance = client.AppsV1Api()
    deployment_result_set = api_instance.list_deployment_for_all_namespaces()

    str_id = str(id)
    id_to_deployment_info_to_dict = {}
    metadata = {}
    spec = {}
    status = {}
    dname = ""
    dnamespace = ""
    duid = ""
    denv_name = ""
    dimage = ""
    dcpu = ""
    dmemory = ""
    dpods = ""
    for item in deployment_result_set.items:
        if item.metadata.uid == str_id:
            metadata = item.metadata.to_dict()
            duid = item.metadata.uid
            dname = item.metadata.name
            dnamespace = item.metadata.namespace

            spec = item.spec.to_dict()
            # denv_name = spec['template']['spec']['containers'][0]['env'][0]['name']
            # dimage = spec['template']['spec']['containers'][0]['image']
            # dcpu = spec['template']['spec']['containers'][0]['resources']['limits']['cpu']
            # dmemory = spec['template']['spec']['containers'][0]['resources']['limits']['memory']
            try:
                container0 = spec['template']['spec']['containers'][0]
                env = container0.get('env')
                denv_name = env[0]['name']
            except Exception as e:
                # print e
                # print "envname"
                denv_name = ""

            try:
                container0 = spec['template']['spec']['containers'][0]
                dimage = container0.get('image')
            except Exception as e:
                # print e
                # print "image"
                dimage = ""

            try:
                # container0 = spec['template']['spec']['containers'][0]['resources']
                # resource = container0.get('limits')
                # dcpu = resource.get('cpu')
                dcpu = spec['template']['spec']['containers'][0]['resources']['limits']['cpu']
            except Exception as e:
                # print e
                # print "cpu"
                dcpu = ""

            try:
                # container0 = spec['template']['spec']['containers'][0]['resources']
                # resource = container0.get('limits')
                # print "aaa", type(resource)
                # dmemory = resource.get('memory')
                dmemory = spec['template']['spec']['containers'][0]['resources']['limits']['memory']
            except Exception as e:
                # print e
                # print "memory"
                dmemory = ""

            dpods = spec['replicas']
            if dpods == None:
                dpods = ""

            status = item.status.to_dict()
            break
    id_deployment_info = {}
    id_deployment_info['metadata'] = metadata
    id_deployment_info['spec'] = spec
    id_deployment_info['status'] = status

    info_for_update = {}
    info_for_update['id'] = duid
    info_for_update['name'] = dname
    info_for_update['namespace'] = dnamespace
    info_for_update['image'] = dimage
    info_for_update['env_name'] = denv_name
    info_for_update['cpu'] = dcpu
    info_for_update['memory'] = dmemory
    info_for_update['pods'] = dpods

    id_to_deployment_info_to_dict['id'] = duid
    id_to_deployment_info_to_dict['info_for_update'] = info_for_update
    id_to_deployment_info_to_dict['id_deployment_info'] = id_deployment_info

    # print id_to_deployment_info_to_dict
    return id_to_deployment_info_to_dict


def delete_deployment_from_id(id):

    # load k8s config-file
    load_k8s_config()
    # create an instance of API class

    # if only can get id info from front-end, so need to search deployment list and get id to (name, namespace)
    api_instance = client.AppsV1Api()
    deployment_result_set = api_instance.list_deployment_for_all_namespaces()
    str_id = str(id)

    for item_deployment in deployment_result_set.items:

        if item_deployment.metadata.uid == str_id:
            delete_name = item_deployment.metadata.name
            delete_namespace = item_deployment.metadata.namespace
            break
    # print test
    # print delete_name
    # print delete_namespace

    # if can get deployment-name and deployment-namespace info from front-end, just exec followed code
    try:
        api_response = api_instance.delete_namespaced_deployment(delete_name, delete_namespace)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling AppsV1Api->delete_namespaced_deployment: %s\n" % e)

"""
def list_all_pods_with_theirIPs():

    load_k8s_config()
    v1 = client.CoreV1Api()
    print "listing pods with their IPs:"
    ret = v1.list_pod_for_all_namespaces(watch=False)
    for item in ret.items:
        print("%s\t%s\t%s" % (item.status.pod_ip, item.metadata.namespace, item.metadata.name) )
"""


def list_all_hadoop_cluster_info():

    load_k8s_config()
    api_instance = client.AppsV1Api()
    cluster_flag = ['HADOOP', 'HBASE', 'SPARK', 'HIVE']
    try:
        hadoop_cluster_deployment_infos = []
        deployment_result_set = api_instance.list_deployment_for_all_namespaces()
        for item in deployment_result_set.items:
            # print item
            containers = item.spec.template.spec.containers
            # print containers
            info = containers[0].to_dict()
            if info['env'] != None:
                ans = info['env']
                info2 = ans[0]
                if info2['name'] != None and info2['name'] in cluster_flag:
                    # this is a hadoop deployment
                    hadoop_cluster_deployment_info = {}
                    hadoop_cluster_deployment_info['id'] = item.metadata.uid
                    hadoop_cluster_deployment_info['name'] = item.metadata.name
                    hadoop_cluster_deployment_info['namespace'] = item.metadata.namespace
                    hadoop_cluster_deployment_info['replicas'] = item.spec.replicas
                    hadoop_cluster_deployment_info['labels'] = item.metadata.labels

                    hadoop_cluster_deployment_info['image'] = item.spec.template.spec.containers[0].image
                    hadoop_cluster_deployment_info['env_name'] = info2['name']

                    resources = item.spec.template.spec.containers[0].resources.to_dict()
                    hadoop_cluster_deployment_info['cpu'] = resources['limits']['cpu']
                    hadoop_cluster_deployment_info['memory'] = resources['limits']['memory']

                    hadoop_cluster_deployment_infos.append(hadoop_cluster_deployment_info)

    except Exception as e:
        print "list_all_hadoop_cluster_info --- exceptions info: "
        print e

    hadoop_cluster_deployment_infos_to_dict = {}
    hadoop_cluster_deployment_infos_to_dict["hadoop_cluster_deployment_infos"] = hadoop_cluster_deployment_infos
    # print json.dumps(hadoop_cluster_deployment_infos_to_dict)
    return json.dumps(hadoop_cluster_deployment_infos_to_dict)

def list_all_hadoop_cluster_info2(who, who_id):

    load_k8s_config()
    api_instance = client.AppsV1Api()
    cluster_flag = ['HADOOP', 'HBASE', 'SPARK', 'HIVE']

    try:
        hadoop_cluster_deployment_infos = []
        deployment_result_set = api_instance.list_deployment_for_all_namespaces()
        if str(who) == 'admin':
            for item in deployment_result_set.items:
                # print item
                containers = item.spec.template.spec.containers
                # print containers
                info = containers[0].to_dict()
                if info['env'] != None:
                    ans = info['env']
                    info2 = ans[0]
                    if info2['name'] != None and info2['name'] in cluster_flag:
                        # this is a hadoop deployment
                        hadoop_cluster_deployment_info = {}
                        hadoop_cluster_deployment_info['id'] = item.metadata.uid
                        hadoop_cluster_deployment_info['name'] = item.metadata.name
                        hadoop_cluster_deployment_info['namespace'] = item.metadata.namespace
                        hadoop_cluster_deployment_info['replicas'] = item.spec.replicas
                        hadoop_cluster_deployment_info['labels'] = item.metadata.labels

                        hadoop_cluster_deployment_info['image'] = item.spec.template.spec.containers[0].image
                        hadoop_cluster_deployment_info['env_name'] = info2['name']

                        resources = item.spec.template.spec.containers[0].resources.to_dict()
                        hadoop_cluster_deployment_info['cpu'] = resources['limits']['cpu']
                        hadoop_cluster_deployment_info['memory'] = resources['limits']['memory']

                        hadoop_cluster_deployment_infos.append(hadoop_cluster_deployment_info)
        else:
            for item in deployment_result_set.items:
                # print item
                print ""
                if item.metadata.namespace == str(who):
                    containers = item.spec.template.spec.containers
                    # print containers
                    info = containers[0].to_dict()
                    if info['env'] != None:
                        ans = info['env']
                        info2 = ans[0]
                        if info2['name'] != None and info2['name'] in cluster_flag:
                            # this is a hadoop deployment
                            hadoop_cluster_deployment_info = {}
                            hadoop_cluster_deployment_info['id'] = item.metadata.uid
                            hadoop_cluster_deployment_info['name'] = item.metadata.name
                            hadoop_cluster_deployment_info['namespace'] = item.metadata.namespace
                            hadoop_cluster_deployment_info['replicas'] = item.spec.replicas
                            hadoop_cluster_deployment_info['labels'] = item.metadata.labels

                            hadoop_cluster_deployment_info['image'] = item.spec.template.spec.containers[0].image
                            hadoop_cluster_deployment_info['env_name'] = info2['name']

                            resources = item.spec.template.spec.containers[0].resources.to_dict()
                            hadoop_cluster_deployment_info['cpu'] = resources['limits']['cpu']
                            hadoop_cluster_deployment_info['memory'] = resources['limits']['memory']

                            hadoop_cluster_deployment_infos.append(hadoop_cluster_deployment_info)

    except Exception as e:
        print "list_all_hadoop_cluster_info --- exceptions info: "
        print e

    hadoop_cluster_deployment_infos_to_dict = {}
    hadoop_cluster_deployment_infos_to_dict["hadoop_cluster_deployment_infos"] = hadoop_cluster_deployment_infos
    # print json.dumps(hadoop_cluster_deployment_infos_to_dict)
    return json.dumps(hadoop_cluster_deployment_infos_to_dict)


# for test
if __name__ == "__main__":
    # list_all_deployment()
    get_deployment_info_from_id2('2bf4aa60-ad22-11e9-a092-44a8423d9a40')