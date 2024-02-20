using a user which
 
select * from Pg_user where usesuper=True
grant the following to the user that needs to see serverless. 
ALTER USER "IAMR:AWSReservedSSO_InfraPlatformEngineer_11830522875b669d" WITH SYSLOG ACCESS UNRESTRICTED;

grant select on svv_redshift_databases to "IAMR:AWSReservedSSO_NFL_Infra_Platform_Engineer_f638160cbb86cd8d";
grant select on svv_all_schemas to IAMR:AWSReservedSSO_NFL_Infra_Platform_Engineer_f638160cbb86cd8d;
grant select on svv_all_tables to IAMR:AWSReservedSSO_NFL_Infra_Platform_Engineer_f638160cbb86cd8d;
