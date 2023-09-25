# Authentication Methods
The Flink kusto connector allows the user to authenticate with AAD using an AAD application,or managed identity based auth.


## AAD Application Authentication
This authentication assumes that an AAD app has been registered and the application id, application secret and the tenant can be used for the auth.

* **setAppId**: AAD application (client) identifier.

* **setTenantId**: AAD authentication authority. This is the AAD Directory (tenant) ID.

* **setAppKey**: AAD application key for the client.

#### Example
```java
KustoConnectionOptions kustoConnectionOptions = KustoConnectionOptions.builder()
  .setAppId(appId)
  .setAppKey(appKey)
  .setTenantId(tenantId)
  .setClusterUrl(cluster).build();
```
## Managed Identity Authentication
Managed identity authentication allows the user to authenticate with AAD using a managed identity. This authentication method supports system managed identity and user managed identity.

* **setManagedIdentityAppId**: The managed identity id to use. Use "system" for system managed identity. Use the object guid for user managed identity.

#### Example
```java
KustoConnectionOptions kustoConnectionOptions = KustoConnectionOptions.builder()
  .setManagedIdentityAppId(appId)
  .setClusterUrl(cluster).build();
```
