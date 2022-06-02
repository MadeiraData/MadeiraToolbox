SELECT
	@@SERVERNAME				 AS server_name
      , @@SERVICENAME				 AS service_name
      , SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS server_bios_name
      , SERVERPROPERTY('MachineName')		 AS machine_name
      , @@VERSION				 AS version_details
      , SERVERPROPERTY('Edition')		 AS edition
      , SERVERPROPERTY('EngineEdition')		 AS engine_edition
      , SERVERPROPERTY('ProductVersion')	 AS product_version
      , SERVERPROPERTY('ProductMajorVersion')	 AS product_major_version
      , SERVERPROPERTY('ProductUpdateLevel')	 AS product_update_level
      , SERVERPROPERTY('ErrorLogFileName')	 AS errorlog_path
      , SERVERPROPERTY('IsClustered')		 AS is_clustered
      , SERVERPROPERTY('IsHadrEnabled')		 AS is_hadrenabled
      , SERVERPROPERTY('LicenseType')		 AS license_type

SELECT
	@@SPID					 AS SPID
      , CONNECTIONPROPERTY('net_transport')	 AS net_transport
      , CONNECTIONPROPERTY('protocol_type')	 AS protocol_type
      , CONNECTIONPROPERTY('auth_scheme')	 AS auth_scheme
      , CONNECTIONPROPERTY('local_net_address')	 AS local_net_address
      , CONNECTIONPROPERTY('local_tcp_port')	 AS local_tcp_port
      , CONNECTIONPROPERTY('client_net_address') AS client_net_address
      , SUSER_SNAME()				 AS login_name
      , SUSER_SID()				 AS login_sid
      , IS_SRVROLEMEMBER('sysadmin')		 AS is_sysadmin
      , HOST_NAME()				 AS host_name
      , APP_NAME()				 AS app_name
      , HOST_ID()				 AS host_process_id
      , DB_NAME()				 AS database_name
      , USER_NAME()				 AS database_user_name
