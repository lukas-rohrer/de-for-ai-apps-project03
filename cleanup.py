# goal: delete redshift cluster and detach roles
import configparser
import boto3


def main():
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('dwh.cfg')

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")



    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )


  
    try:
        print('Deleting Redshift cluster.')
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

        waiter = redshift.get_waiter('cluster_deleted')
        waiter.wait(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MaxRecords=100,
            WaiterConfig={
                'Delay': 100,
                'MaxAttempts': 100
            }
        )
        print('Cluster successfully deleted.')

    except Exception as e:
        print(e)
    

    try:
        print('Detatching roles.')
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    except Exception as e:
        print(e)


    config["DWH"]["DWH_ENDPOINT"] = ""
    config["IAM_ROLE"]["ARN"] = ""
    config['AWS']['KEY'] = ""
    config['AWS']['SECRET'] = ""

    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile, space_around_delimiters=False)


    
if __name__ == "__main__":
    main()