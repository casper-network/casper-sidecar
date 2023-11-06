# Casper Event Sidecar USAGE

This document describes how to consume events and perform queries using the Sidecar, covering the following topics:

- Node-generated events emitted by the node(s) to which the Sidecar connects
- Sidecar-generated events originating solely from the Sidecar service and not from a node
- The RESTful endpoint for performing useful queries about the state of the network

## Prerequisites

* Run the service as described in the [README](README.md).

## The Sidecar Event Stream

The Sidecar event stream is a passthrough for all the events emitted by the node(s) to which the Sidecar connects. This stream also includes one endpoint for Sidecar-generated events that can be useful, although the node did not emit them.

Events are divided into four categories and emitted on their respective endpoints:

- **Deploy events** - Associated with Deploys on a node and emitted on the `events/deploys` endpoint. Currently, only a `DeployAccepted` event is emitted. The URL to consume these events using Sidecar on a Mainnet or Testnet node is `http://<HOST>:19999/events/deploys/`.
- **Finality Signature events** - Emitted on the `events/sigs` endpoint when a block has been finalized and cannot be altered. The URL to consume finality signature events using Sidecar on a Mainnet or Testnet node is `http://<HOST>:19999/events/sigs/`.
- **Main events** - All other events are emitted on the `events/main` endpoint, including `BlockAdded`, `DeployProcessed`, `DeployExpired`, `Fault`, and `Step` events. The URL to consume these events using Sidecar on a Mainnet or Testnet node is `http://<HOST>:19999/events/main/`.
- **Sidecar-generated events** - The Sidecar also emits events on the `events/sidecar` endpoint, designated for events originating solely from the Sidecar service. The URL to consume these events using Sidecar on a Mainnet or Testnet node is `http://<HOST>:19999/events/sidecar/`.

For more information on various event types emitted by the node, visit the [Monitoring and Consuming Events](https://docs.casperlabs.io/developers/dapps/monitor-and-consume-events/#event-types) documentation.

### Monitoring the Sidecar Event Stream

It is possible to monitor the Sidecar event stream using *cURL*, depending on how the HOST and PORT are configured.

```json
curl -s http://<HOST:PORT>/events/<TYPE>
```

- `HOST` - The IP address where the Sidecar is running
- `PORT` - The port number where the Sidecar emits events
- `TYPE` - The type of event emitted

Given this [example configuration](EXAMPLE_NODE_CONFIG.toml), here are the commands for each endpoint:

- **Deploy events:** 

    ```json
    curl -sN http://127.0.0.1:19999/events/deploys
    ```

- **Finality Signature events:** 

    ```json
    curl -sN http://127.0.0.1:19999/events/sigs
    ```

- **Main events:** 

    ```json
    curl -sN http://127.0.0.1:19999/events/main
    ```

- **Sidecar-generated events:** 

    ```json
    curl -sN http://127.0.0.1:19999/events/sidecar
    ```

### The API Version of Node Events

An `ApiVersion` event is always emitted when a new client connects to a node's SSE server, informing the client of the node's software version.

When a client connects to the Sidecar, the Sidecar displays the node’s API version, `ApiVersion`, which it receives from the node. Then, it starts streaming the events coming from the node. The `ApiVersion` may differ from the node’s build version.

If the node goes offline, the `ApiVersion` may differ when it restarts (i.e., in the case of an upgrade). In this case, the Sidecar will report the new `ApiVersion` to its client. If the node’s `ApiVersion` has not changed, the Sidecar will not report the version again and will continue to stream messages that use the previous version.

Here is an example of what the API version would look like while listening on the Sidecar’s `DeployAccepted` event stream:

```
curl -sN http://127.0.0.1:19999/events/deploys

data:{"ApiVersion":"1.4.8"}

data:{"DeployAccepted":{"hash":"00eea4fb9baa37af401cba8ffb96a1b96d594234908cb5f9de50effcb5b1c5aa","header":{"account":"0202ed20f3a93b5386bc41b6945722b2bd4250c48f5fa0632adf546e2f3ff6f4ddee","timestamp":"2023-02-28T12:21:14.604Z","ttl":"30m","gas_price":1,"body_hash":"f06261b964600caf712a3ea0dc54448c3fcc008638368580eb4de6832dce8698","dependencies":[],"chain_name":"casper"},"payment":{"ModuleBytes":{"module_bytes":"","args":[["amount",{"cl_type":"U512","bytes":"0400e1f505","parsed":"100000000"}]]}},"session":{"Transfer":{"args":[["amount",{"cl_type":"U512","bytes":"05205d59d832","parsed":"218378100000"}],["target",{"cl_type":{"ByteArray":32},"bytes":"6fbe4634d42aa1ae7820eed35bcbd5c687de5c464e5348650b49a21a17c8dcb5","parsed":"6fbe4634d42aa1ae7820eed35bcbd5c687de5c464e5348650b49a21a17c8dcb5"}],["id",{"cl_type":{"Option":"U64"},"bytes":"00","parsed":null}]]}},"approvals":[{"signer":"0202ed20f3a93b5386bc41b6945722b2bd4250c48f5fa0632adf546e2f3ff6f4ddee","signature":"02b519ecb34f954aeb7afede122c6f999b2124022f6b653304b2891c5428b074795ad9232a409aa0d3e601471331ea50143ca4c378306ffcd0f8ff7a60e13f19db"}]}}
id:21821471

:

:

:
```

#### Middleware Mode

The Sidecar can connect simultaneously to nodes with different build versions, which send messages with different API versions. There is also the rare possibility of nodes changing API versions and not being in sync with other connected nodes. Although this situation would be rare, clients should be able to parse messages with different API versions.

### The Version of Sidecar Events

When a client connects to the `events/sidecar` endpoint, it will receive a message containing the version of the Sidecar software. Release version `1.1.0` would look like this:

```
curl -sN http://127.0.0.1:19999/events/sidecar

data:{"SidecarVersion":"1.1.0"}

:

:

```

Note that the SidecarVersion differs from the APIVersion emitted by the node event streams. You will also see the keep-alive messages as colons, ensuring the connection is active.

### The Node Shutdown Event

When the node sends a Shutdown event and disconnects from the Sidecar, the Sidecar will report it as part of the event stream and on the `/events/deploys` endpoint. The Sidecar will continue to operate and attempt to reconnect to the node according to the `max_attempts` and `delay_between_retries_in_seconds` settings specified in its configuration.

The Sidecar does not expose Shutdown events via its REST API. 

Here is an example of how the stream might look like if the node went offline for an upgrade and came back online after a Shutdown event with a new `ApiVersion`:

```
curl -sN http://127.0.0.1:19999/events/deploys

data:{"ApiVersion":"1.5.2"}

data:{"BlockAdded":{"block_hash":"b487aae22b406e303d96fc44b092f993df6f3b43ceee7b7f5b1f361f676492d6","block":{"hash":"b487aae22b406e303d96fc44b092f993df6f3b43ceee7b7f5b1f361f676492d6","header":{"parent_hash":"4a28718301a83a43563ec42a184294725b8dd188aad7a9fceb8a2fa1400c680e","state_root_hash":"63274671f2a860e39bb029d289e688526e4828b70c79c678649748e5e376cb07","body_hash":"6da90c09f3fc4559d27b9fff59ab2453be5752260b07aec65e0e3a61734f656a","random_bit":true,"accumulated_seed":"c8b4f30a3e3e082f4f206f972e423ffb23d152ca34241ff94ba76189716b61da","era_end":{"era_report":{"equivocators":[],"rewards":{"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80":1559401400039,"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8":25895190891},"inactive_validators":[]},"next_era_validator_weights":{"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80":"50538244651768072","010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8":"839230678448335"}},"timestamp":"2021-04-08T05:14:14.912Z","era_id":90,"height":1679394427512,"protocol_version":"1.0.0"},"body":{"proposer":"012bac1d0ff9240ff0b7b06d555815640497861619ca12583ddef434885416e69b","deploy_hashes":[],"transfer_hashes":[]}}}}
id:1

:

data:"Shutdown"
id:2

:

:

:

data:{"ApiVersion":"1.5.2"}

data:{"BlockAdded":{"block_hash":"1c76e7abf5780b49d3a66beef7b75bbf261834f494dededb8f2e349735659c03","block":{"hash":"1c76e7abf5780b49d3a66beef7b75bbf261834f494dededb8f2e349735659c03","header":{"parent_hash":"4a28718301a83a43563ec42a184294725b8dd188aad7a9fceb8a2fa1400c680e","state_root_hash":"63274671f2a860e39bb029d289e688526e4828b70c79c678649748e5e376cb07","body_hash":"6da90c09f3fc4559d27b9fff59ab2453be5752260b07aec65e0e3a61734f656a","random_bit":true,"accumulated_seed":"c8b4f30a3e3e082f4f206f972e423ffb23d152ca34241ff94ba76189716b61da","era_end":{"era_report":{"equivocators":[],"rewards":[{"validator":"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80","amount":1559401400039},{"validator":"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8","amount":25895190891}],"inactive_validators":[]},"next_era_validator_weights":[{"validator":"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80","weight":"50538244651768072"},{"validator":"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8","weight":"839230678448335"}]},"timestamp":"2021-04-08T05:14:14.912Z","era_id":90,"height":1679394457791,"protocol_version":"1.0.0"},"body":{"proposer":"012bac1d0ff9240ff0b7b06d555815640497861619ca12583ddef434885416e69b","deploy_hashes":[],"transfer_hashes":[]},"proofs":[]}}}
id:3

:

:

```

Note that the Sidecar can emit another type of shutdown event on the `events/sidecar` endpoint, as described below.

### The Sidecar Shutdown Event

If the Sidecar attempts to connect to a node that does not come back online within the maximum number of reconnection attempts, the Sidecar will start a controlled shutdown process. It will emit a Sidecar-specific Shutdown event on the [events/sidecar](#the-sidecar-shutdown-event) endpoint, designated for events originating solely from the Sidecar service. The other event streams do not get this message because they only emit messages from the node.

The message structure of the Sidecar shutdown event is the same as the [node shutdown event](#the-node-shutdown-event). The sidecar event stream would look like this:

```
curl -sN http://127.0.0.1:19999/events/sidecar

data:{"SidecarVersion":"1.1.0"}

:

:

:

data:"Shutdown"
id:8
```

## The REST Server

The Sidecar provides a RESTful endpoint for useful queries about the state of the network.

### Latest Block

Retrieve information about the last block added to the linear chain.

The path URL is `<HOST:PORT>/block`.

Example:

```json
curl -s http://127.0.0.1:18888/block
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"block_hash":"95b0d7b7e94eb79a7d2c79f66e2324474fc8f54536b9e6b447413fa6d00c2581","block":{"hash":"95b0d7b7e94eb79a7d2c79f66e2324474fc8f54536b9e6b447413fa6d00c2581","header":{"parent_hash":"48a99605ed4d1b27f9ddf8a1a0819c576bec57dd7a1b105247e48a5165b4194b","state_root_hash":"8d439b84b62e0a30f8e115047ce31c5ddeb30bd46eba3de9715412c2979be26e","body_hash":"b34c6c6ea69669597578a1912548ef823f627fe667ddcdb6bcd000acd27c7a2f","random_bit":true,"accumulated_seed":"058b14c76832b32e8cd00750e767c60f407fb13b3b0c1e63aea2d6526202924d","era_end":null,"timestamp":"2022-11-20T12:44:22.912Z","era_id":7173,"height":1277846,"protocol_version":"1.5.2"},"body":{"proposer":"0169e1552a97843ff2ef4318e8a028a9f4ed0c16b3d96f6a6eee21e6ca0d4022bc","deploy_hashes":[],"transfer_hashes":["d2193e27d6f269a6f4e0ede0cca805baa861d553df8c9f438cc7af56acf40c2b"]},"proofs":[]}}
```
</details>
<br></br>


### Block by Hash

Retrieve information about a block given its block hash.

The path URL is `<HOST:PORT>/block/<block-hash>`. Enter a valid block hash. 

Example:

```json
curl -s http://127.0.0.1:18888/block/96a989a7f4514909b442faba3acbf643378fb7f57f9c9e32013fdfad64e3c8a5
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"block_hash":"96a989a7f4514909b442faba3acbf643378fb7f57f9c9e32013fdfad64e3c8a5","block":{"hash":"96a989a7f4514909b442faba3acbf643378fb7f57f9c9e32013fdfad64e3c8a5","header":{"parent_hash":"8f29120995ae6942d1a48cc4ac8dc3be5de5886f1fb53140356c907f1a70d7ef","state_root_hash":"c8964dddfe3660f481f750c5acd776fe7e08c1e168a4184707d07da6bac5397c","body_hash":"31984faf50cfb2b96774e388a16407cbf362b66d22e1d55201cc0709fa3e1803","random_bit":false,"accumulated_seed":"5ce60583fc1a8b3da07900b7223636eadd97ea8eef6abec28cdbe4b3326c1d6c","era_end":null,"timestamp":"2022-11-20T18:36:05.504Z","era_id":7175,"height":1278485,"protocol_version":"1.5.2"},"body":{"proposer":"017de9688caedd0718baed968179ddbe0b0532a8ef0a9a1cb9dfabe9b0f6016fa8","deploy_hashes":[],"transfer_hashes":[]},"proofs":[]}}
```
</details>
<br></br>

### Block by Height

Retrieve information about a block, given a specific block height.

The path URL is `<HOST:PORT>/block/<block-height>`. Enter a valid number representing the block height.

Example:

```json
curl -s http://127.0.0.1:18888/block/1278485
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"block_hash":"96a989a7f4514909b442faba3acbf643378fb7f57f9c9e32013fdfad64e3c8a5","block":{"hash":"96a989a7f4514909b442faba3acbf643378fb7f57f9c9e32013fdfad64e3c8a5","header":{"parent_hash":"8f29120995ae6942d1a48cc4ac8dc3be5de5886f1fb53140356c907f1a70d7ef","state_root_hash":"c8964dddfe3660f481f750c5acd776fe7e08c1e168a4184707d07da6bac5397c","body_hash":"31984faf50cfb2b96774e388a16407cbf362b66d22e1d55201cc0709fa3e1803","random_bit":false,"accumulated_seed":"5ce60583fc1a8b3da07900b7223636eadd97ea8eef6abec28cdbe4b3326c1d6c","era_end":null,"timestamp":"2022-11-20T18:36:05.504Z","era_id":7175,"height":1278485,"protocol_version":"1.5.2"},"body":{"proposer":"017de9688caedd0718baed968179ddbe0b0532a8ef0a9a1cb9dfabe9b0f6016fa8","deploy_hashes":[],"transfer_hashes":[]},"proofs":[]}}
```
</details>
<br></br>

### Deploy by Hash

Retrieve an aggregate of the various states a deploy goes through, given its deploy hash. The node does not emit this event, but the Sidecar computes it and returns it for the given deploy. This endpoint behaves differently than other endpoints, which return the raw event received from the node. 

The path URL is `<HOST:PORT>/deploy/<deploy-hash>`. Enter a valid deploy hash. 

The output differs depending on the deploy's status, which changes over time as the deploy goes through its [lifecycle](https://docs.casperlabs.io/concepts/design/casper-design/#execution-semantics-phases).

Example:

```json
curl -s http://127.0.0.1:18888/deploy/8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7
```

The sample output below is for a deploy that was accepted but has yet to be processed.

<details> 
<summary><b>Deploy accepted but not processed yet</b></summary>

```json
{"deploy_hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","deploy_accepted":{"hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","header":{"account":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","timestamp":"2022-11-20T22:33:59.786Z","ttl":"1h","gas_price":1,"body_hash":"c0c3dedaaac4c962a966376c124cf2225df9c8efce4c2af05c4181be661f41aa","dependencies":[],"chain_name":"casper"},"payment":{"ModuleBytes":{"module_bytes":"","args":[["amount",{"cl_type":"U512","bytes":"0410200395","parsed":"2500010000"}]]}},"session":{"StoredContractByHash":{"hash":"ccb576d6ce6dec84a551e48f0d0b7af89ddba44c7390b690036257a04a3ae9ea","entry_point":"add_bid","args":[["public_key",{"cl_type":"PublicKey","bytes":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","parsed":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a"}],["amount",{"cl_type":"U512","bytes":"05008aa69516","parsed":"97000000000"}],["delegation_rate",{"cl_type":"U8","bytes":"00","parsed":0}]]}},"approvals":[{"signer":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","signature":"01a7ff7affdc13fac7436acf1b6d7c2282fff0f9185ebe1ce97f2e510b20d0375ad07eaca46f8d72f342e7b9e50a39c2eaf75da0c63365abfd526bbaffa4d33f02"}]},"deploy_processed":{},"deploy_expired":false}
```
</details>
<br></br>

The next sample output is for a deploy that was accepted and processed.

<details> 
<summary><b>Deploy accepted and processed successfully</b></summary>

```json
{"deploy_hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","deploy_accepted":{"hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","header":{"account":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","timestamp":"2022-11-20T22:33:59.786Z","ttl":"1h","gas_price":1,"body_hash":"c0c3dedaaac4c962a966376c124cf2225df9c8efce4c2af05c4181be661f41aa","dependencies":[],"chain_name":"casper"},"payment":{"ModuleBytes":{"module_bytes":"","args":[["amount",{"cl_type":"U512","bytes":"0410200395","parsed":"2500010000"}]]}},"session":{"StoredContractByHash":{"hash":"ccb576d6ce6dec84a551e48f0d0b7af89ddba44c7390b690036257a04a3ae9ea","entry_point":"add_bid","args":[["public_key",{"cl_type":"PublicKey","bytes":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","parsed":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a"}],["amount",{"cl_type":"U512","bytes":"05008aa69516","parsed":"97000000000"}],["delegation_rate",{"cl_type":"U8","bytes":"00","parsed":0}]]}},"approvals":[{"signer":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","signature":"01a7ff7affdc13fac7436acf1b6d7c2282fff0f9185ebe1ce97f2e510b20d0375ad07eaca46f8d72f342e7b9e50a39c2eaf75da0c63365abfd526bbaffa4d33f02"}]},"deploy_processed":{"deploy_hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","account":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","timestamp":"2022-11-20T22:33:59.786Z","ttl":"1h","dependencies":[],"block_hash":"2caea6929fe4bd615f5c7451ecddc607a99d7512c85add4fe816bd4ee88fce63","execution_result":{"Success":{"effect":{"operations":[],"transforms":[{"key":"hash-d2469afeb99130f0be7c9ce230a84149e6d756e306ef8cf5b8a49d5182e41676","transform":"Identity"},{"key":"hash-d63c44078a1931b5dc4b80a7a0ec586164fd0470ce9f8b23f6d93b9e86c5944d","transform":"Identity"},{"key":"hash-7cc1b1db4e08bbfe7bacf8e1ad828a5d9bcccbb33e55d322808c3a88da53213a","transform":"Identity"},{"key":"hash-4475016098705466254edd18d267a9dad43e341d4dafadb507d0fe3cf2d4a74b","transform":"Identity"},{"key":"balance-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd","transform":"Identity"},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":"Identity"},{"key":"balance-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd","transform":{"WriteCLValue":{"cl_type":"U512","bytes":"05f0c773b316","parsed":"97499990000"}}},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":{"AddUInt512":"2500010000"}},{"key":"hash-ccb576d6ce6dec84a551e48f0d0b7af89ddba44c7390b690036257a04a3ae9ea","transform":"Identity"},{"key":"hash-86f2d45f024d7bb7fb5266b2390d7c253b588a0a16ebd946a60cb4314600af74","transform":"Identity"},{"key":"hash-7cc1b1db4e08bbfe7bacf8e1ad828a5d9bcccbb33e55d322808c3a88da53213a","transform":"Identity"},{"key":"hash-4475016098705466254edd18d267a9dad43e341d4dafadb507d0fe3cf2d4a74b","transform":"Identity"},{"key":"uref-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915-000","transform":{"WriteCLValue":{"cl_type":"Unit","bytes":"","parsed":null}}},{"key":"balance-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915","transform":{"WriteCLValue":{"cl_type":"U512","bytes":"00","parsed":"0"}}},{"key":"hash-7cc1b1db4e08bbfe7bacf8e1ad828a5d9bcccbb33e55d322808c3a88da53213a","transform":"Identity"},{"key":"hash-4475016098705466254edd18d267a9dad43e341d4dafadb507d0fe3cf2d4a74b","transform":"Identity"},{"key":"balance-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd","transform":"Identity"},{"key":"balance-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915","transform":"Identity"},{"key":"balance-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd","transform":{"WriteCLValue":{"cl_type":"U512","bytes":"04f03dcd1d","parsed":"499990000"}}},{"key":"balance-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915","transform":{"AddUInt512":"97000000000"}},{"key":"transfer-1e75292a29d210326d8845082b302037300eac92c7d2612790ca3ab1a62e570d","transform":{"WriteTransfer":{"deploy_hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","from":"account-hash-eb1dd0668899cf6b35cf99f5d4a7d3ea05acf352f75d14075982e0aebc099776","to":"account-hash-6174cf2e6f8fed1715c9a3bace9c50bfe572eecb763b0ed3f644532616452008","source":"uref-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd-007","target":"uref-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915-007","amount":"97000000000","gas":"0","id":null}}},{"key":"bid-eb1dd0668899cf6b35cf99f5d4a7d3ea05acf352f75d14075982e0aebc099776","transform":{"WriteBid":{"validator_public_key":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","bonding_purse":"uref-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915-007","staked_amount":"97000000000","delegation_rate":0,"vesting_schedule":null,"delegators":{},"inactive":false}}},{"key":"deploy-8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","transform":{"WriteDeployInfo":{"deploy_hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","transfers":["transfer-1e75292a29d210326d8845082b302037300eac92c7d2612790ca3ab1a62e570d"],"from":"account-hash-eb1dd0668899cf6b35cf99f5d4a7d3ea05acf352f75d14075982e0aebc099776","source":"uref-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd-007","gas":"2500000000"}}},{"key":"hash-d2469afeb99130f0be7c9ce230a84149e6d756e306ef8cf5b8a49d5182e41676","transform":"Identity"},{"key":"hash-d63c44078a1931b5dc4b80a7a0ec586164fd0470ce9f8b23f6d93b9e86c5944d","transform":"Identity"},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":"Identity"},{"key":"hash-d2469afeb99130f0be7c9ce230a84149e6d756e306ef8cf5b8a49d5182e41676","transform":"Identity"},{"key":"hash-7cc1b1db4e08bbfe7bacf8e1ad828a5d9bcccbb33e55d322808c3a88da53213a","transform":"Identity"},{"key":"hash-4475016098705466254edd18d267a9dad43e341d4dafadb507d0fe3cf2d4a74b","transform":"Identity"},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":"Identity"},{"key":"balance-8c2ffb7e82c5a323a4e50f6eea9a080feb89c71bb2db001bde7449e13328c0dc","transform":"Identity"},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":{"WriteCLValue":{"cl_type":"U512","bytes":"00","parsed":"0"}}},{"key":"balance-8c2ffb7e82c5a323a4e50f6eea9a080feb89c71bb2db001bde7449e13328c0dc","transform":{"AddUInt512":"2500010000"}}]},"transfers":["transfer-1e75292a29d210326d8845082b302037300eac92c7d2612790ca3ab1a62e570d"],"cost":"2500000000"}}},"deploy_expired":false}
```
</details>
<br></br>

### Accepted Deploy by Hash

Retrieve information about an accepted deploy, given its deploy hash.

The path URL is `<HOST:PORT>/deploy/accepted/<deploy-hash>`. Enter a valid deploy hash.

Example:

```json
curl -s http://127.0.0.1:18888/deploy/accepted/8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","header":{"account":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","timestamp":"2022-11-20T22:33:59.786Z","ttl":"1h","gas_price":1,"body_hash":"c0c3dedaaac4c962a966376c124cf2225df9c8efce4c2af05c4181be661f41aa","dependencies":[],"chain_name":"casper"},"payment":{"ModuleBytes":{"module_bytes":"","args":[["amount",{"cl_type":"U512","bytes":"0410200395","parsed":"2500010000"}]]}},"session":{"StoredContractByHash":{"hash":"ccb576d6ce6dec84a551e48f0d0b7af89ddba44c7390b690036257a04a3ae9ea","entry_point":"add_bid","args":[["public_key",{"cl_type":"PublicKey","bytes":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","parsed":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a"}],["amount",{"cl_type":"U512","bytes":"05008aa69516","parsed":"97000000000"}],["delegation_rate",{"cl_type":"U8","bytes":"00","parsed":0}]]}},"approvals":[{"signer":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","signature":"01a7ff7affdc13fac7436acf1b6d7c2282fff0f9185ebe1ce97f2e510b20d0375ad07eaca46f8d72f342e7b9e50a39c2eaf75da0c63365abfd526bbaffa4d33f02"}]}
```
</details>
<br></br>


### Expired Deploy by Hash

Retrieve information about a deploy that expired, given its deploy hash.

The path URL is `<HOST:PORT>/deploy/expired/<deploy-hash>`. Enter a valid deploy hash.

Example:

```json
curl -s http://127.0.0.1:18888/deploy/expired/e03544d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab58a
```

### Processed Deploy by Hash

Retrieve information about a deploy that was processed, given its deploy hash.
The path URL is `<HOST:PORT>/deploy/processed/<deploy-hash>`. Enter a valid deploy hash.

Example:

```json
curl -s http://127.0.0.1:18888/deploy/processed/8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"deploy_hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","account":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","timestamp":"2022-11-20T22:33:59.786Z","ttl":"1h","dependencies":[],"block_hash":"2caea6929fe4bd615f5c7451ecddc607a99d7512c85add4fe816bd4ee88fce63","execution_result":{"Success":{"effect":{"operations":[],"transforms":[{"key":"hash-d2469afeb99130f0be7c9ce230a84149e6d756e306ef8cf5b8a49d5182e41676","transform":"Identity"},{"key":"hash-d63c44078a1931b5dc4b80a7a0ec586164fd0470ce9f8b23f6d93b9e86c5944d","transform":"Identity"},{"key":"hash-7cc1b1db4e08bbfe7bacf8e1ad828a5d9bcccbb33e55d322808c3a88da53213a","transform":"Identity"},{"key":"hash-4475016098705466254edd18d267a9dad43e341d4dafadb507d0fe3cf2d4a74b","transform":"Identity"},{"key":"balance-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd","transform":"Identity"},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":"Identity"},{"key":"balance-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd","transform":{"WriteCLValue":{"cl_type":"U512","bytes":"05f0c773b316","parsed":"97499990000"}}},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":{"AddUInt512":"2500010000"}},{"key":"hash-ccb576d6ce6dec84a551e48f0d0b7af89ddba44c7390b690036257a04a3ae9ea","transform":"Identity"},{"key":"hash-86f2d45f024d7bb7fb5266b2390d7c253b588a0a16ebd946a60cb4314600af74","transform":"Identity"},{"key":"hash-7cc1b1db4e08bbfe7bacf8e1ad828a5d9bcccbb33e55d322808c3a88da53213a","transform":"Identity"},{"key":"hash-4475016098705466254edd18d267a9dad43e341d4dafadb507d0fe3cf2d4a74b","transform":"Identity"},{"key":"uref-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915-000","transform":{"WriteCLValue":{"cl_type":"Unit","bytes":"","parsed":null}}},{"key":"balance-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915","transform":{"WriteCLValue":{"cl_type":"U512","bytes":"00","parsed":"0"}}},{"key":"hash-7cc1b1db4e08bbfe7bacf8e1ad828a5d9bcccbb33e55d322808c3a88da53213a","transform":"Identity"},{"key":"hash-4475016098705466254edd18d267a9dad43e341d4dafadb507d0fe3cf2d4a74b","transform":"Identity"},{"key":"balance-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd","transform":"Identity"},{"key":"balance-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915","transform":"Identity"},{"key":"balance-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd","transform":{"WriteCLValue":{"cl_type":"U512","bytes":"04f03dcd1d","parsed":"499990000"}}},{"key":"balance-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915","transform":{"AddUInt512":"97000000000"}},{"key":"transfer-1e75292a29d210326d8845082b302037300eac92c7d2612790ca3ab1a62e570d","transform":{"WriteTransfer":{"deploy_hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","from":"account-hash-eb1dd0668899cf6b35cf99f5d4a7d3ea05acf352f75d14075982e0aebc099776","to":"account-hash-6174cf2e6f8fed1715c9a3bace9c50bfe572eecb763b0ed3f644532616452008","source":"uref-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd-007","target":"uref-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915-007","amount":"97000000000","gas":"0","id":null}}},{"key":"bid-eb1dd0668899cf6b35cf99f5d4a7d3ea05acf352f75d14075982e0aebc099776","transform":{"WriteBid":{"validator_public_key":"01786c83c59eba29e1f4ae4ee601040970665a816ac5bf856108222b72723f782a","bonding_purse":"uref-3d52e976454512999aee042c3c298474a9d3fa98db80879052465c8a4c57c915-007","staked_amount":"97000000000","delegation_rate":0,"vesting_schedule":null,"delegators":{},"inactive":false}}},{"key":"deploy-8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","transform":{"WriteDeployInfo":{"deploy_hash":"8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7","transfers":["transfer-1e75292a29d210326d8845082b302037300eac92c7d2612790ca3ab1a62e570d"],"from":"account-hash-eb1dd0668899cf6b35cf99f5d4a7d3ea05acf352f75d14075982e0aebc099776","source":"uref-c182f2fafc6eb59306f971a3d3ad06e4ffa09364ca9de2fc48d123e40da243cd-007","gas":"2500000000"}}},{"key":"hash-d2469afeb99130f0be7c9ce230a84149e6d756e306ef8cf5b8a49d5182e41676","transform":"Identity"},{"key":"hash-d63c44078a1931b5dc4b80a7a0ec586164fd0470ce9f8b23f6d93b9e86c5944d","transform":"Identity"},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":"Identity"},{"key":"hash-d2469afeb99130f0be7c9ce230a84149e6d756e306ef8cf5b8a49d5182e41676","transform":"Identity"},{"key":"hash-7cc1b1db4e08bbfe7bacf8e1ad828a5d9bcccbb33e55d322808c3a88da53213a","transform":"Identity"},{"key":"hash-4475016098705466254edd18d267a9dad43e341d4dafadb507d0fe3cf2d4a74b","transform":"Identity"},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":"Identity"},{"key":"balance-8c2ffb7e82c5a323a4e50f6eea9a080feb89c71bb2db001bde7449e13328c0dc","transform":"Identity"},{"key":"balance-fe327f9815a1d016e1143db85e25a86341883949fd75ac1c1e7408a26c5b62ef","transform":{"WriteCLValue":{"cl_type":"U512","bytes":"00","parsed":"0"}}},{"key":"balance-8c2ffb7e82c5a323a4e50f6eea9a080feb89c71bb2db001bde7449e13328c0dc","transform":{"AddUInt512":"2500010000"}}]},"transfers":["transfer-1e75292a29d210326d8845082b302037300eac92c7d2612790ca3ab1a62e570d"],"cost":"2500000000"}}}
```

</details>
<br></br>

### Faults by Public Key

Retrieve the faults associated with a validator's public key.
The path URL is `<HOST:PORT>/faults/<public-key>`. Enter a valid hexadecimal representation of a validator's public key.

Example:

```json
curl -s http://127.0.0.1:18888/faults/01a601840126a0363a6048bfcbb0492ab5a313a1a19dc4c695650d8f3b51302703
```

### Faults by Era

Return the faults associated with an era, given a valid era identifier.
The path URL is: `<HOST:PORT>/faults/<era-ID>`. Enter an era identifier.

Example:

```json
curl -s http://127.0.0.1:18888/faults/2304
```

### Finality Signatures by Block

Retrieve the finality signatures in a block, given its block hash. 

The path URL is: `<HOST:PORT>/signatures/<block-hash>`. Enter a valid block hash.

Example:

```json
curl -s http://127.0.0.1:18888/signatures/85aa2a939bc3a4afc6d953c965bab333bb5e53185b96bb07b52c295164046da2
```

### Step by Era

Retrieve the step event emitted at the end of an era, given a valid era identifier.

The path URL is: `<HOST:PORT>/step/<era-ID>`. Enter a valid era identifier.

Example:

```json
curl -s http://127.0.0.1:18888/step/7268
```

### Missing Filter

If no filter URL was specified after the root address (HOST:PORT), an error message will be returned.

Example:

```json
curl http://127.0.0.1:18888
{"code":400,"message":"Invalid request path provided"}
```

### Invalid Filter

If an invalid filter was specified, an error message will be returned.

Example:

```json
curl http://127.0.0.1:18888/other
{"code":400,"message":"Invalid request path provided"}
```
