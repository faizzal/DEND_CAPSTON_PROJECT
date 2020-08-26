import pandas as pd
import boto3
import json
import configparser
import time

import os
'''
this run server is bash python file helpping to create and start cluster redshift server
is help users to run the server without using cli command or UI AWS with all setting 
if you want test this by your cradintal information (login key and secret key )
have free to change the cfg variable information 
'''   
THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
my_file = os.path.join(THIS_FOLDER, 'dwh.cfg')

config = configparser.ConfigParser()
config.read_file(open(my_file))

'''
setVariables is function that load variables from cfg file into python class
'''

def setVariables():

        KEY                    = config.get('AWS','KEY')
        SECRET                 = config.get('AWS','SECRET')

        DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
        DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
        DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
        DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

        DWH_DB                 = config.get("CLUSTER","DWH_DB")
        DWH_DB_USER            = config.get("CLUSTER","DWH_DB_USER")
        DWH_DB_PASSWORD        = config.get("CLUSTER","DWH_DB_PASSWORD")
        DWH_PORT               = config.get("CLUSTER","DWH_PORT") 
        
        ARN                    = config.get("IAM_ROLE","ARN") 
        (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

        df = pd.DataFrame({"Param":
                          ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB",
                           "DWH_DB_USER","DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                      "Value":
                          [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD,
                           DWH_PORT, DWH_IAM_ROLE_NAME]
                     })
        print("=============START SET VARIABLES==============")
        print(df)
        print("===========SET VARIABLES COMPLETE=============")
        return  (KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, ARN)

 '''
SetUpResources is function that setting resource for AWS cresorces
'''   
    
def SetUpResources(KEY, SECRET):
    
    print("=============START SET IAM==============")
    iam = boto3.client('iam',aws_access_key_id=KEY, aws_secret_access_key= SECRET, region_name='us-west-2')
    print("=============END SET IAM==============")
   
    print("=============START SET ec2==============")
    
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    print("=============END SET ec2==============")

    print("=============START SET S3==============")

    s3 = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                       )
    print("=============END SET S3==============")
   
    print("=============START SET redshift==============")

    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
    print("=============START SET redshift==============")
    return(iam, ec2, s3, redshift)
    
    
#create IAM    
def creating_IAM(iam, DWH_IAM_ROLE_NAME):
    #1.1 Create the role, 
        try:
            print("1.1 Creating a new IAM Role") 
            dwhRole = iam.create_role(
                Path='/',
                RoleName=DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )    
        except Exception as e:
            print(e)


        print("1.2 Attaching Policy")

        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']

        print("1.3 Get the IAM role ARN")
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

        print(roleArn)
        config.set('IAM_ROLE', 'ARN', roleArn)

        
def create_redshift(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, ARN):
 
    try:
        
            response = redshift.create_cluster (        
                #HW
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=int(DWH_NUM_NODES),

                #Identifiers & Credentials
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,


                # TODO: add parameter for role (to allow s3 access)
                IamRoles=[ARN]
            )
    except Exception as e:
            print(e)
    
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def open_tcp(ec2, myClusterProps, DWH_PORT):
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
            print(e)


def main():
    print("---------------------set up variables--------------------------\n")
    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, ARN = setVariables()
    print("---------------------set up resources--------------------------\n")
    iam, ec2, s3, redshift = SetUpResources(KEY, SECRET)
    
    print("---------------------create IAM---------------------------------\n")
    
    creating_IAM(iam, DWH_IAM_ROLE_NAME)
    
    
    print("---------------------create redshift cluster--------------------------\n")
    create_redshift(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, ARN)
    
    print("---------------------create redshift--------------------------\n")
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print(myClusterProps["ClusterStatus"])

    while myClusterProps["ClusterStatus"] != 'available':
        print("The Redshift Cluster is .... "+myClusterProps["ClusterStatus"])
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        time.sleep(10)
    else:
        print("---------------------cluster is Avalible  --------------------------\n")
        pdf=prettyRedshiftProps(myClusterProps)
        print(pdf)
       
            
    
    open_tcp(ec2, myClusterProps, DWH_PORT)
    
if __name__ == "__main__":
    main()