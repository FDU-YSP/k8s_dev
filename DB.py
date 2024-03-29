import MySQLdb
import json
from zun_ui.api import k8s_client
from kubernetes import client, config
from django.conf import settings

# def create_connect_tomysql_DB():
#     # config database info
#     info = getattr(settings, "MYSQL_DB_CONNECTION_INFO", None)
#     try:
#         if info.equals(None):
#             raise
#         else:
#             ip = info['IP']
#             user = info['USER']
#             pw = info['PASSWORD']
#             dbname = info['DBNAME']
#             # db = MySQLdb.connect("10.10.87.66", "root", "Qwer`123", "hadoop", charset='utf8')
#             db = MySQLdb.connect(ip, user, pw, dbname, charset='utf8')
#             # return a database connection
#             return db
#     except:
#         print "local_settings.py not have DB_CONNECTION_INFO !!!"
#         # return None


def create_connect_tomysql_DB():
    # config database info
    info = getattr(settings, "MYSQL_DB_CONNECTION_INFO", None)
    ip = info['IP']
    user = info['USER']
    pw = info['PASSWORD']
    dbname = info['DBNAME']
    # db = MySQLdb.connect("10.10.87.66", "root", "Qwer`123", "hadoop", charset='utf8')
    db = MySQLdb.connect(ip, user, pw, dbname, charset='utf8')
    # return a database connection
    return db

def read_hadoop_info_from_id(id):
    db = create_connect_tomysql_DB()
    conn = db.cursor()

    # load k8s config-file
    k8s_client.load_k8s_config()
    # create an instance of API class
    api_instance = client.AppsV1Api()
    deployment_result_set = api_instance.list_deployment_for_all_namespaces()
    # str_id = str(id)

    for item_deployment in deployment_result_set.items:

        if item_deployment.metadata.uid == str(id):
            name = item_deployment.metadata.name
            namespace = item_deployment.metadata.namespace
            break
    # print name, namespace
    metadata = {}
    try:
        select_hadoop_info_sql = "SELECT * FROM hadoop_cluster WHERE cluster_name = '" + name + "' AND namespace = '" + \
                                 namespace + "'"
        conn.execute(select_hadoop_info_sql)
        results_set = conn.fetchall()
        for row in results_set:
            metadata['cluster_name'] = row[0]
            metadata['namespace'] = row[7]
            metadata['create_time'] = row[3]
            metadata['number_of_nodes'] = row[1]
            metadata['available_number'] = row[2]
            metadata['cluster_status'] = row[6]
            metadata['master_node_id'] = row[4]
            metadata['slave_node_id'] = row[5]
            break
        # print json.dumps(metadata)

    except Exception as e:
        print "select hadoop_cluster info from database --- exception info: "
        print e

    # print metadata

    master_node_id = metadata['master_node_id']
    select_master_node_info_sql = "SELECT * FROM hadoop_nodes WHERE container_id  = '" + master_node_id + "'"
    conn.execute(select_master_node_info_sql)
    results_set2 = conn.fetchall()
    hadoop_master_node = {}
    for row in results_set2:
        hadoop_master_node['container_id'] = row[3]
        hadoop_master_node['name'] = row[1]
        hadoop_master_node['namespace'] = row[0]
        hadoop_master_node['ip'] = row[4]
        hadoop_master_node['status'] = row[5]
        break
    # print json.dumps(hadoop_master_node)

    slave_node_id = metadata['slave_node_id']
    slave_node_id_str = str(slave_node_id)
    slave_node_ids = slave_node_id_str.split('@-@')
    # print slave_node_ids
    lens = len(slave_node_ids)
    ids = ""
    for i in range(0, lens):
        ids = ids + '\'' + slave_node_ids[i] + '\''
        if i<lens-1:
            ids = ids + ','

    select_slave_node_info_sql = "SELECT * FROM hadoop_nodes WHERE container_id in ("
    select_slave_node_info_sql += ids
    select_slave_node_info_sql += ')'
    conn.execute(select_slave_node_info_sql)
    results_set3 = conn.fetchall()

    hadoop_slave_node = []
    for row in results_set3:
        # print row
        hadoop_slave_node_item = {}
        hadoop_slave_node_item['container_id'] = row[3]
        hadoop_slave_node_item['name'] = row[1]
        hadoop_slave_node_item['namespace'] = row[0]
        hadoop_slave_node_item['ip'] = row[4]
        hadoop_slave_node_item['status'] = row[5]
        hadoop_slave_node.append(hadoop_slave_node_item)

    # print json.dumps(hadoop_slave_node)
    hadoop_cluster_info = {}
    hadoop_cluster_info['metadata'] = metadata
    hadoop_cluster_info['hadoop_master_node'] = hadoop_master_node
    hadoop_cluster_info['hadoop_slave_node'] = hadoop_slave_node
    hadoop_cluster_info_to_dict = {}
    hadoop_cluster_info_to_dict['hadoop_cluster_info'] = hadoop_cluster_info
    hadoop_cluster_info_to_dict['id'] = id

    # close database connection
    db.close()
    # print json.dumps(hadoop_cluster_info_to_dict)
    return hadoop_cluster_info_to_dict

# qh
def read_hadoop_info_from_clustername(clustername):
    db = create_connect_tomysql_DB()
    conn = db.cursor()
    name = clustername

    try:
        select_hadoop_info_sql = 'SELECT * FROM hadoop_cluster WHERE cluster_name = "' + name + '"'
        conn.execute(select_hadoop_info_sql)
        results_set = conn.fetchall()
        metadata = {}
        for row in results_set:
            metadata['cluster_name'] = row[0]
            metadata['namespace'] = row[7]
            metadata['create_time'] = row[3]
            metadata['number_of_nodes '] = row[1]
            metadata['available_number'] = row[2]
            metadata['cluster_status'] = row[6]
            metadata['master_node_id'] = row[4]
            metadata['slave_node_id'] = row[5]
            break
        # print json.dumps(metadata)

    except Exception as e:
        print 'read_hadoop_info_from_clustername --- exception info: '
        print e

    print metadata

    master_node_id = metadata['master_node_id']
    select_master_node_info_sql = "SELECT * FROM hadoop_nodes WHERE container_id  = '" + master_node_id + "'"
    conn.execute(select_master_node_info_sql)
    results_set2 = conn.fetchall()
    hadoop_master_node = {}
    for row in results_set2:
        hadoop_master_node['container_id'] = row[3]
        hadoop_master_node['name'] = row[1]
        hadoop_master_node['namespace'] = row[0]
        hadoop_master_node['ip'] = row[4]
        hadoop_master_node['status'] = row[5]
        break
    # print json.dumps(hadoop_master_node)

    slave_node_id = metadata['slave_node_id']
    slave_node_id_str = str(slave_node_id)
    slave_node_ids = slave_node_id_str.split('@-@')
    # print slave_node_ids
    lens = len(slave_node_ids)
    ids = ""
    for i in range(0, lens):
        ids = ids + '\'' + slave_node_ids[i] + '\''
        if i < lens - 1:
            ids = ids + ','

    select_slave_node_info_sql = 'SELECT * FROM hadoop_nodes WHERE container_id in ('
    select_slave_node_info_sql += ids
    select_slave_node_info_sql += ')'
    conn.execute(select_slave_node_info_sql)
    results_set3 = conn.fetchall()

    hadoop_slave_node = []
    for row in results_set3:
        # print row
        hadoop_slave_node_item = {}
        hadoop_slave_node_item['container_id'] = row[3]
        hadoop_slave_node_item['name'] = row[1]
        hadoop_slave_node_item['namespace'] = row[0]
        hadoop_slave_node_item['ip'] = row[4]
        hadoop_slave_node_item['status'] = row[5]
        hadoop_slave_node.append(hadoop_slave_node_item)

    # print json.dumps(hadoop_slave_node)
    hadoop_cluster_info = {}
    hadoop_cluster_info['metadata'] = metadata
    hadoop_cluster_info['hadoop_master_node'] = hadoop_master_node
    hadoop_cluster_info['hadoop_slave_node'] = hadoop_slave_node
    hadoop_cluster_info_to_dict = {}
    hadoop_cluster_info_to_dict['hadoop_cluster_info'] = hadoop_cluster_info
    # json.dumps(hadoop_cluster_info_to_dict)

    db.close()
    return hadoop_cluster_info_to_dict

def get_master_details_from_clustername(clustername):
    clusterdetail = read_hadoop_info_from_clustername(clustername)
    masterdetails = clusterdetail['hadoop_cluster_info']['hadoop_master_node']
    return masterdetails


def get_all_cluster():
    db = create_connect_tomysql_DB()
    conn = db.cursor()
    select_hadoop_info_sql = 'SELECT cluster_name FROM hadoop_cluster'
    conn.execute(select_hadoop_info_sql)
    results_set = conn.fetchall()
    clusterlist = []
    for row in results_set:
        row = "".join(tuple(row))
        clusterlist.append(row)

    db.close()
    return clusterlist


# select cluster
def get_master_info_from_name_and_namespace(name, namespace):
    db = create_connect_tomysql_DB()
    conn = db.cursor()
    select_master_container_sql = "SELECT hadoop_master_id FROM hadoop_cluster WHERE cluster_name = '" + name + "' AND namespace = '" \
                        + namespace + "'"
    conn.execute(select_master_container_sql)
    results_set = conn.fetchall()
    container_id = ""
    for row in results_set:
        container_id = row[0]
        break

    print container_id
    select_master_info_sql = "SELECT name, container_id, ip FROM  hadoop_nodes WHERE container_id = '" + container_id + "'"
    conn.execute(select_master_info_sql)
    results_set2 = conn.fetchall()
    master_info = {}
    for row in results_set2:
        master_info['name'] = row[0]
        master_info['container_id'] = row[1]
        master_info['ip'] = row[2]
        break
    return master_info

# for test
if __name__ == '__main__':
    s = 'hadoop-cluster'
    print get_master_details_from_clustername(s)
