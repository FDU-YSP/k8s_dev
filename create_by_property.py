from zun_ui.api import k8s_client
from os import path
import yaml
from kubernetes import client, config
from django.conf import settings

def create_deployment_object(dname, dnamespace, dreplicas, dimage, denv_name, dcpu, dmemory):

    str_dname = str(dname)
    str_dnamespace = str(dnamespace)
    str_dreplicas = str(dreplicas)
    str_dimage = str(dimage)
    str_denv_name = str(denv_name)
    str_dcpu = str(dcpu)
    str_dmemory = str(dmemory)

    # Configureate Pod template container
    container = client.V1Container(
        name=str.lower(str_denv_name),
        image=str_dimage,
        # image_pull_policy="IfNotPresent",
        env=[client.V1EnvVar(name=str_denv_name, value=str_dreplicas)],
        resources={"limits": {"cpu": str_dcpu, "memory": str_dmemory}})
    # ports=[client.V1ContainerPort(container_port=80)])

    # Create and configurate a spec section
    # get nodename from horizon local_settings.py
    node_name_ = getattr(settings, "NODE_NAME", None)
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"clusterType": str_denv_name}),
        spec=client.V1PodSpec(containers=[container], node_name=node_name_))

    # Create the specification of deployment
    spec = client.ExtensionsV1beta1DeploymentSpec(
        replicas=int(str_dreplicas),
        template=template)

    # Instantiate the deployment object
    deployment = client.ExtensionsV1beta1Deployment(
        api_version="extensions/v1beta1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name=str_dname, namespace=str_dnamespace),
        spec=spec
    )
    return deployment


def create_deployment(api_instance, deployment, dnamespace):
    # Create deployement
    api_response = api_instance.create_namespaced_deployment(
        body=deployment,
        namespace=dnamespace)
    print("Deployment created. status='%s'" % str(api_response.status))

"""
def update_deployment(api_instance, deployment):
    # Update container image
    deployment.spec.template.spec.containers[0].image = "nginx:1.9.1"
    # Update the deployment
    api_response = api_instance.patch_namespaced_deployment(
        name=DEPLOYMENT_NAME,
        namespace="default",
        body=deployment)
    print("Deployment updated. status='%s'" % str(api_response.status))
"""

def update_deployment(update_property):
    # update replicas
    k8s_client.load_k8s_config()
    api_instance = client.ExtensionsV1beta1Api()

    deployments = api_instance.list_deployment_for_all_namespaces()
    id = update_property['id']
    for item in deployments.items:
        if item.metadata.uid == str(id):
            name = item.metadata.name
            namespace = item.metadata.namespace
            deployment = item
            break;

    # update this deployment
    deployment.spec.replicas = update_property['pods']
    api_response = api_instance.patch_namespaced_deployment(
        name=name,
        namespace=namespace,
        body=deployment)
    print("Deployment updated. status='%s'" % str(api_response.status))
    return

"""
 def delete_deployment(api_instance):
    # Delete deployment
    api_response = api_instance.delete_namespaced_deployment(
        name=DEPLOYMENT_NAME,
        namespace="default",
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    print("Deployment deleted. status='%s'" % str(api_response.status))
"""

def main(create_property):

    k8s_client.load_k8s_config()
    extensions_v1beta1 = client.ExtensionsV1beta1Api()

    dnm = create_property['name']
    dnp = create_property['namespace']
    dpods = create_property['pods']
    dimage = create_property['image']
    denv_name = create_property['env_name']
    dcpu = create_property['cpu']
    dmemory = create_property['memory']

    deployment = create_deployment_object(dnm, dnp, dpods, dimage, denv_name, dcpu, dmemory)
    create_deployment(extensions_v1beta1, deployment, dnp)
    return

if __name__ == "__main__":
    update_deployment()
