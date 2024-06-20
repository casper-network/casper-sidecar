# Casper Sidecar USAGE

This document describes how to consume events and perform queries using the Sidecar, covering the following topics:

- Node-generated events emitted by the node(s) to which the Sidecar connects
- Sidecar-generated events originating solely from the Sidecar service and not from a node
- The RESTful endpoint for performing useful queries about the state of the network

## Prerequisites

* Run the service as described in the [README](README.md).

## The Sidecar Event Stream

The Sidecar event stream is a passthrough for all the events emitted by the node(s) to which the Sidecar connects. This stream also includes one endpoint for Sidecar-generated events that can be useful, although the node did not emit them.

Events are emitted on two endpoints: 
* All events that come from a node are re-emitted under `http://<HOST>:<SIDECAR_SSE_PORT>/events`.
* All Sidecar-generated events reporting the Sidecar's internal state are emitted under `http://<HOST>:<SIDECAR_SSE_PORT>/events/sidecar`.

For more information on various event types emitted by the node, visit the [Monitoring and Consuming Events](https://docs.casperlabs.io/developers/dapps/monitor-and-consume-events/#event-types) documentation.

### Monitoring the Sidecar event stream

It is possible to monitor the Sidecar event stream using *cURL*, depending on how the HOST and PORT are configured.

The Sidecar can connect to Casper nodes with versions greater or equal to `2.0.0`.

```sh
curl -s http://<HOST:PORT>/events
```

- `HOST` - The IP address where the Sidecar is running
- `PORT` - The port number where the Sidecar emits events

Given this [example configuration](./resources/example_configs/EXAMPLE_NODE_CONFIG.toml), here are the commands for each endpoint:

```sh
curl -sN http://127.0.0.1:19999/events
```

Also, the Sidecar exposes an endpoint for Sidecar-generated events:

```sh
curl -sN http://127.0.0.1:19999/events/sidecar
```

### Node events versioning

An `ApiVersion` event is always emitted when the Sidecar connects to a node's SSE server, broadcasting the node's software version. Then, the Sidecar starts streaming the events coming from the node. Note that the `ApiVersion` may differ from the node’s build version.

If the node goes offline, the `ApiVersion` may differ when it restarts (i.e., in the case of an upgrade). In this case, the Sidecar will report the new `ApiVersion` to its client. If the node’s `ApiVersion` has not changed, the Sidecar will not report the version again and will continue to stream messages that use the previous version.

Here is an example of what the API version would look like while listening on the Sidecar’s event stream. The colons represent "keep-alive" messages.

```sh
curl -sN http://127.0.0.1:19999/events

data:{"ApiVersion":"2.0.0"}

data:{"TransactionProcessed": {"transaction_hash": {"Version1": "56642d06d9642c512a7bf55413108ce65bfd1105361bf36ff3586998529e116b" }, "initiator_addr": {"PublicKey": "014962b395b25a89cf970340fb51da2adbfb0f5836716e26dbae6754e79e01ab68" }, "timestamp": "2020-08-07T01:22:10.209Z", "ttl": "11h 9m 50s 128ms", "block_hash": "08ad20808db3098e4461182d18c6efd68db6b01f4e22d4005bfdc4f007a7c0d0", "execution_result": {"Version1": {"Failure": {"effect": {"operations": [], "transforms": [ {"key": "12570563858918177191", "transform": "Identity" }, {"key": "14635000063685912943", "transform": {"AddUInt64": 5592565879698622687 } } ] }, "transfers": [ "transfer-9a9304069e5a68e408824ba9a16a99bb50179926f58023371ef82cc9565d68fb" ], "cost": "3760779910350774860", "error_message": "Error message 15494687491298509010" } } }, "messages": [ {"entity_addr": "addressable-entity-68c22b361a3a74f49dde2873f93d8485e9a08cc14c7f154b46a25435ca8ef449", "message": {"String": "Va6WL5U9dFhLbG3HCJQvuqcA46EslCY9fymlYbHqpvFlo4PeUs0nUVgeXavUIYc7" }, "block_index": 0, "topic_index": 0, "topic_name": "QnlypxwtJpoTOF8opgGiuGYseeNvcU5A", "topic_name_hash": "e9c77898578d8d1e5063cf3c7c60ca048b8176f10ba1684c2f05961a152acfa7", "index": 3213106390 }, {"entity_addr": "addressable-entity-063582249fa5823b94f883f6c784e3b5b9742780b7fa7c0549823be7debc7680", "message": {"String": "nwOXDbkcq5xEyDxONQizPdBIpWpPi1SBtLCws0a3F0v1nu7FyjbvjErKOjAYYwg0" }, "block_index": 1, "topic_index": 1, "topic_name": "hQLIE3k8zWLnslrmN9RRROhLk4g2LxeQ", "topic_name_hash": "1747f053151847f43ae3b8cac607dc7bb672aa3aec1c2bbb7e3a866613fe3803", "index": 1507321819 }, {"entity_addr": "addressable-entity-acbce74845514977568693e79876f60a9fd0459a4419cc8392820cce7c25ca8e", "message": {"String": "w7aWCBO3uIjQf91hjSFZ6xog0w8b6HyPAVW5iBUFVx7XWPOho7tLrw6a3DpJMA9o" }, "block_index": 2, "topic_index": 2, "topic_name": "TNXBnGjXCGANWJK4YSvD5HUZnoWRQGRn", "topic_name_hash": "f7c3f5fa51fd729bc3af86f724e764e66efae425aa47025ec0dd88f8c062baad", "index": 1303188972 }, {"entity_addr": "addressable-entity-8f1f553f3ca14a9510557cd85e42a7e0269d4a344e74cf1e83d9751e875559f0", "message": {"String": "ZJsXLKE3V08ihPnxZxtZmDffb68zl6A4vsVQsYkSCm8Tvg8RCGNXRWOR6c12zphq" }, "block_index": 3, "topic_index": 3, "topic_name": "sfYdJVcjs68cwCpd9pSeQ7NwWdvLi2Q0", "topic_name_hash": "16b791cf5685e45ecf6a41c3442173ca2bf6c8b6971ada579420b3e28803c992", "index": 1637472264 } ] }}
id:21821471

:

:

:
```

>**Note**: The Sidecar can connect simultaneously to nodes with different build versions, which send messages with different API versions. There is also the rare possibility of nodes changing API versions and not being in sync with other connected nodes. Although this situation would be rare, clients should be able to parse messages with different API versions.

### Sidecar events versioning

When a client connects to the `events/sidecar` endpoint, it will receive a message containing the version of the Sidecar software. Release version `1.1.0` would look like this:

```sh
curl -sN http://127.0.0.1:19999/events/sidecar

data:{"SidecarVersion":"1.1.0"}

:

:
```

Note that the SidecarVersion differs from the APIVersion emitted by the node event streams. You will also see the keep-alive messages as colons, ensuring the connection is active.

### The node's Shutdown event

When the node sends a Shutdown event and disconnects from the Sidecar, the Sidecar will report it as part of the event stream on the `/events` endpoint. The Sidecar will continue to operate and attempt to reconnect to the node according to the `max_attempts` and `delay_between_retries_in_seconds` settings specified in its configuration.

The Sidecar does not expose Shutdown events via its REST API. 

Here is an example of how the stream might look like if the node went offline for an upgrade and came back online after a Shutdown event with a new `ApiVersion`:

```sh
curl -sN http://127.0.0.1:19999/events

data:{"ApiVersion":"2.0.0"}

data:{"BlockAdded":{"block_hash":"bb5332a4f0feae6a760d67b3e2a24adf4599aaf6845584f20d80f037e2505f69","block":{"Version2":{"hash":"bb5332a4f0feae6a760d67b3e2a24adf4599aaf6845584f20d80f037e2505f69","header":{"parent_hash":"4c1fb7a23f0de75e14ef5077dbf6ffedbdf2c4a26c2e5890f2694be1be9c78de","state_root_hash":"e7e75dd4500801195276096ffe274973e8da2b73430138bd4d9c1804f658d277","body_hash":"a8f9c258f7276ca6ab2788c5df78ac4a94480a327de9d4675c2b528bb0e7faed","random_bit":true,"accumulated_seed":"630d9b48148044845d91867646685a3a85ec2ddc11634a935aa0b22e248bc17d","era_end":null,"timestamp":"2024-03-19T15:17:09.163Z","era_id":178172,"height":1781728,"protocol_version":"2.0.0"},"body":{"proposer":"0202b55941afeb1ec56170b12752f5a592e3d8fe222e4f9830eca538e667c790f2ae","mint":[],"auction":[],"install_upgrade":[],"standard":[],"rewarded_signatures":[]}}}}}
id:1

:

data:"Shutdown"
id:2

:

:

:

data:{"ApiVersion":"2.0.1"}

data:{"BlockAdded":{"block_hash":"8d7b333799ed9d0dd8764d75947c618ae0a198cf6551e4026521011b31a53934","block":{"Version2":{"hash":"8d7b333799ed9d0dd8764d75947c618ae0a198cf6551e4026521011b31a53934","header":{"parent_hash":"98789674cd19222df62d9bf7293642a6193ad60eec204802cd1f3ea9a601a8af","state_root_hash":"d4386260b30b66704c6d99c70b01afe09671f29b8cb6ed69afae0abeef4a84e3","body_hash":"85210d3bf069c9f534b4af9c8ddc8cd63ef971f4c9d7f4d3dcbc57c5164a0737","random_bit":true,"accumulated_seed":"2787dcda83de66d13502aad716ac4469efda1f3072bece0c11bd902d3cdcbeaa","era_end":null,"timestamp":"2024-03-20T14:45:55.936Z","era_id":895818,"height":8958184,"protocol_version":"1.0.0","current_gas_price":1},"body":{"proposer":"014e6a488e8cb7c64ee7ca1263e8b3df15e8e5cc28512bd7d5a17fd210d00b0947","mint":[],"auction":[],"install_upgrade":[],"standard":[],"rewarded_signatures":[]}}}}}
id:3

:

:
```

Note that the Sidecar can emit another type of shutdown event on the `events/sidecar` endpoint, as described below.

### The Sidecar Shutdown event

If the Sidecar attempts to connect to a node that does not come back online within the maximum number of reconnection attempts, the Sidecar will start a controlled shutdown process. It will emit a Sidecar-specific Shutdown event on the [events/sidecar](#the-sidecar-shutdown-event) endpoint, designated for events originating solely from the Sidecar service. The other event streams do not get this message because they only emit messages from the node.

The message structure of the Sidecar shutdown event is the same as the [node shutdown event](#the-node-shutdown-event). The Sidecar event stream would look like this:

```sh
curl -sN http://127.0.0.1:19999/events/sidecar

data:{"SidecarVersion":"1.1.0"}

:

:

:

data:"Shutdown"
id:8
```

## Replaying the Event Stream

This command will replay the event stream from an old event onward. The server will replay all the cached events if the ID is 0 or if you specify an event ID already purged from the node's cache.

Replace the `HOST`, `PORT`, and `ID` fields with the values needed.

```sh
curl -sN http://HOST:PORT/events?start_from=ID
```

**Example:**

```sh
curl -sN http://65.21.235.219:9999/events?start_from=29267508
```

Note that certain shells like `zsh` may require an escape character before the question mark.

## The REST Server

The Sidecar provides a RESTful endpoint for useful queries about the state of the network.

### Latest block

Retrieve information about the last block added to the linear chain.

The path URL is `<HOST:PORT>/block`.

Example:

```sh
curl -s http://127.0.0.1:18888/block
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"block_hash":"d32550922798f6f70499f171030d30b12c2cde967f72cff98a0f987663789f89","block":{"Version2":{"hash":"d32550922798f6f70499f171030d30b12c2cde967f72cff98a0f987663789f89","header":{"parent_hash":"676a0a1a5b3e57c1710ccc379b788b4e81773b19c8f4586387a15288c914b1de","state_root_hash":"e25977c41e7a0cea644508ddda67de0837beac112c422dee45ada119b445f188","body_hash":"1c28072d52682b36616a32c44a261c1b44ad386cf9139df2c10c6f1a31584747","random_bit":false,"accumulated_seed":"cfd7817242fe89bcfe4e74cd7122d43047c247ac064151cd10d21c82d62be676","era_end":null,"timestamp":"2024-03-20T09:26:25.460Z","era_id":773313,"height":7733136,"protocol_version":"2.0.0"},"body":{"proposer":"02037f0605b63fe1ee16e852d45fc223b1196602d2028e5dd4ea90ad8e0b0d7006c1","mint":[],"auction":[],"install_upgrade":[],"standard":[],"rewarded_signatures":[]}}}}
```
</details>
<br></br>


### Block by hash

Retrieve information about a block given its block hash.

The path URL is `<HOST:PORT>/block/<block-hash>`. Enter a valid block hash. 

Example:

```sh
curl -s http://127.0.0.1:18888/block/bd2e0c36150a74f50d9884e38a0955f8b1cba94821b9828c5f54d8929d6151bc
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"block_hash":"bd2e0c36150a74f50d9884e38a0955f8b1cba94821b9828c5f54d8929d6151bc","block":{"Version2":{"hash":"bd2e0c36150a74f50d9884e38a0955f8b1cba94821b9828c5f54d8929d6151bc","header":{"parent_hash":"9fffc8f07c11910721850f696fbcc73eb1e9152f333d51d495a45b1b71b4262d","state_root_hash":"190b1c706a65f04e6a8777faa11011d28aefc3830facfeddd4fea5dd06274411","body_hash":"720e4822481a4a14ffd9175bb88d2f9a9976d527f0f9c72c515ab73c99a97cb8","random_bit":true,"accumulated_seed":"8bb2a7a8e973574adb81faa6a7853051a26024bc6a9af80178e372a40edadbff","era_end":null,"timestamp":"2024-03-20T09:27:04.342Z","era_id":644446,"height":6444466,"protocol_version":"2.0.0"},"body":{"proposer":"0203e58aea33501ce2e28c2e30f88d176755fbf9cd3724c6e0f0e7a1733368db3384","mint":[],"auction":[],"install_upgrade":[],"standard":[],"rewarded_signatures":[]}}}}
```
</details>
<br></br>

### Block by chain height

Retrieve information about a block, given a specific block height.

The path URL is `<HOST:PORT>/block/<block-height>`. Enter a valid number representing the block height.

Example:

```sh
curl -s http://127.0.0.1:18888/block/336460
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"block_hash":"2c1a1bda792d123d8ccdcf61b2c9a5bb9a467dc387fa9c85fa708dbf00d7efca","block":{"Version2":{"hash":"2c1a1bda792d123d8ccdcf61b2c9a5bb9a467dc387fa9c85fa708dbf00d7efca","header":{"parent_hash":"77641e387a0ccf4372a0339292984ba6be4b0c3f8b79d7f69f1781c53854dd0f","state_root_hash":"383ea1fe76047e2315ead460bd0d13c0a55adad0dc4bd84782b45c97593b8e32","body_hash":"7e6c19c940988ff42f862af86ccfa17768c93e1821d4ff3feefa250c17e0785c","random_bit":true,"accumulated_seed":"7c053fa1625b5670561f6d59dd83c7057567b8bc89025ba78e37908e3c2c7622","era_end":null,"timestamp":"2024-03-20T09:27:58.468Z","era_id":33646,"height":336460,"protocol_version":"2.0.0"},"body":{"proposer":"01657f46b1f8f8db69a85b41e9b957e9c3d67695ba62f8645b5b01c605d2642925","mint":[],"auction":[],"install_upgrade":[],"standard":[],"rewarded_signatures":[]}}}}
```
</details>
<br></br>

### Transaction by hash

Retrieve an aggregate of the various states a transaction goes through, given its transaction hash. The endpoint also needs the transaction type as an input (`deploy` or `version1`) The node does not emit this event, but the Sidecar computes it and returns it for the given transaction. This endpoint behaves differently than other endpoints, which return the raw event received from the node. 

The path URL is `<HOST:PORT>/transaction/<transaction-type>/<transaction-hash>`. Enter a valid transaction hash. 

The output differs depending on the transaction's status, which changes over time as the transaction goes through its [lifecycle](https://docs.casperlabs.io/concepts/design/casper-design/#execution-semantics-phases).

Example:

```sh
curl -s http://127.0.0.1:18888//transaction/version1/3141e85f8075c3a75c2a1abcc79810c07d103ff97c03200ab0d0baf91995fe4a
```

The sample output below is for a transaction that was accepted but has yet to be processed.

<details> 
<summary><b>Transaction accepted but not processed yet</b></summary>

```json
{"transaction_hash": "3141e85f8075c3a75c2a1abcc79810c07d103ff97c03200ab0d0baf91995fe4a","transaction_accepted": {"header": {"api_version": "2.0.0","network_name": "casper-net-1"},"payload": {"transaction": {"Version1": {"hash": "3141e85f8075c3a75c2a1abcc79810c07d103ff97c03200ab0d0baf91995fe4a","header": {"chain_name": "casper-net-1","timestamp": "2024-03-20T13:31:59.772Z","ttl": "30m","body_hash": "40c7476a175fb97656ec6da1ace2f1900a9d353f1637943a30edd5385494b345","pricing_mode": {"Fixed": {"gas_price_tolerance": 1000}},"initiator_addr": {"PublicKey": "01d848e225db95e34328ca1c64d73ecda50f5070fd6b21037453e532d085a81973"}},"body": {"args": [],"target": {"Session": {"kind": "Standard","module_bytes":"<REDACTED>","runtime": "VmCasperV1"}},"entry_point": {"Custom": "test"},"scheduling": "Standard"},"approvals": [{"signer": "01d848e225db95e34328ca1c64d73ecda50f5070fd6b21037453e532d085a81973","signature": "0154fd295f5d4d62544f63d70470de28b2bf2cddecac2a237b6a2a78d25ee14b21ea2861d711a51f57b3f9f74e247a8d26861eceead6569f233949864a9d5fa100"}]}}}},"transaction_processed": ,"transaction_expired": false}
```
</details>
<br></br>

The next sample output is for a transaction that was accepted and processed.

<details> 
<summary><b>Transaction accepted and processed successfully</b></summary>

```json
{"transaction_hash": "3141e85f8075c3a75c2a1abcc79810c07d103ff97c03200ab0d0baf91995fe4a","transaction_accepted": {"header": {"api_version": "2.0.0","network_name": "casper-net-1"},"payload": {"transaction": {"Version1": {"hash": "3141e85f8075c3a75c2a1abcc79810c07d103ff97c03200ab0d0baf91995fe4a","header": {"chain_name": "casper-net-1","timestamp": "2024-03-20T13:31:59.772Z","ttl": "30m","body_hash": "40c7476a175fb97656ec6da1ace2f1900a9d353f1637943a30edd5385494b345","pricing_mode": {"Fixed": {"gas_price_tolerance": 1000}},"initiator_addr": {"PublicKey": "01d848e225db95e34328ca1c64d73ecda50f5070fd6b21037453e532d085a81973"}},"body": {"args": [],"target": {"Session": {"kind": "Standard","module_bytes":"<REDACTED>","runtime": "VmCasperV1"}},"entry_point": {"Custom": "test"},"scheduling": "Standard"},"approvals": [{"signer": "01d848e225db95e34328ca1c64d73ecda50f5070fd6b21037453e532d085a81973","signature": "0154fd295f5d4d62544f63d70470de28b2bf2cddecac2a237b6a2a78d25ee14b21ea2861d711a51f57b3f9f74e247a8d26861eceead6569f233949864a9d5fa100"}]}}}},"transaction_processed": {"transaction_hash":{"Deploy":"c6907d46a5cc61ef30c66dbb6599208a57d3d62812c5f061169cdd7ad4e52597"},"initiator_addr":{"PublicKey":"0202dec9e70126ddd13af6e2e14771339c22f73626202a28ef1ed41594a3b2a79156"},"timestamp":"2024-03-20T13:58:57.301Z","ttl":"2m 53s","block_hash":"6c6a1fb17147fe467a52f8078e4c6d1143e8f61e2ec0c57938a0ac5f49e3f960","execution_result":{"Version1":{"Success":{"effect":{"operations":[{"key":"9192013132486795888","kind":"NoOp"}],"transforms":[{"key":"9278390014984155010","transform":{"AddUInt64":17967007786823421753}},{"key":"8284631679508534160","transform":{"AddUInt512":"13486131286369918968"}},{"key":"11406903664472624400","transform":{"AddKeys":[{"name":"5532223989822042950","key":"6376159234520705888"},{"name":"9797089120764120320","key":"3973583116099652644"},{"name":"17360643427404656075","key":"3412027808185329863"},{"name":"9849256366384177518","key":"1556404389498537987"},{"name":"14237913702817074429","key":"16416969798013966173"}]}},{"key":"11567235260771335457","transform":"Identity"},{"key":"13285707355579107355","transform":"Identity"}]},"transfers":[],"cost":"14667737366273622842"}}},"messages":[{"entity_addr":{"SmartContract":[193,43,184,185,6,88,15,83,243,107,130,63,136,174,24,148,79,214,87,238,171,138,195,141,119,235,134,196,253,221,36,0]},"message":{"String":"wLNta4zbpJiW5ScjagPXm5LoGViYApCfIbEXJycPUuLQP4fA7REhV4LdBRbZ7bQb"},"topic_name":"FdRRgbXEGS1xKEXCJKvaq7hVyZ2ZUlSb","topic_name_hash":"473f644238bbb334843df5bd06a85e8bc34d692cce804de5f97e7f344595c769","topic_index":4225483688,"block_index":16248749308130060594},{"entity_addr":{"Account":[109,75,111,241,219,141,104,160,197,208,7,245,112,199,31,150,68,65,166,247,43,111,0,56,32,124,7,36,107,230,100,132]},"message":{"String":"U5qR82wJoPDGJWhwJ4qkblsu6Q5DDqDt0Q2pAjhVOUjn520PdvYOC27oo4aDEosw"},"topic_name":"zMEkHxGgUUSMmb7eWJhFs5e6DH9vXvCg","topic_name_hash":"d911ebafb53ccfeaf5c970e462a864622ec4e3a1030a17a8cfaf4d7a4cd74d48","topic_index":560585407,"block_index":15889379229443860143}]},"transaction_expired": false}
```

</details>
<br></br>

### Accepted transaction by hash

Retrieve information about an accepted transaction, given its transaction hash.

The path URL is `<HOST:PORT>/transaction/accepted/<transaction-type>/<transaction-hash>`. Enter a valid transaction hash.

Example:

```sh
curl -s http://127.0.0.1:18888/transaction/accepted/version1/8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"header": {"api_version": "2.0.0","network_name": "casper-net-1"},"payload": {"transaction": {"Version1": {"hash": "3141e85f8075c3a75c2a1abcc79810c07d103ff97c03200ab0d0baf91995fe4a","header": {"chain_name": "casper-net-1","timestamp": "2024-03-20T13:31:59.772Z","ttl": "30m","body_hash": "40c7476a175fb97656ec6da1ace2f1900a9d353f1637943a30edd5385494b345","pricing_mode": {"Fixed": {"gas_price_tolerance": 1000}},"initiator_addr": {"PublicKey": "01d848e225db95e34328ca1c64d73ecda50f5070fd6b21037453e532d085a81973"}},"body": {"args": [],"target": {"Session": {"kind": "Standard","module_bytes": "<REDACTED>","runtime": "VmCasperV1"}},"entry_point": {"Custom": "test"},"scheduling": "Standard"},"approvals": [{"signer": "01d848e225db95e34328ca1c64d73ecda50f5070fd6b21037453e532d085a81973","signature": "0154fd295f5d4d62544f63d70470de28b2bf2cddecac2a237b6a2a78d25ee14b21ea2861d711a51f57b3f9f74e247a8d26861eceead6569f233949864a9d5fa100"}]}}}}
```
</details>
<br></br>


### Expired transaction by hash

Retrieve information about a transaction that expired, given its trnasaction type and transaction hash.

The path URL is `<HOST:PORT>/transaction/expired/<transaction-type>/<transaction-hash>`. Enter a valid transaction hash.

Example:

```sh
curl -s http://127.0.0.1:18888/transaction/expired/version1/3dcf9cb73977a1163129cb0801163323bea2a780815bc9dc46696a43c00e658c
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"header": {"api_version": "2.0.0","network_name": "some-network"},"payload": {"transaction_hash": {"Version1": "3dcf9cb73977a1163129cb0801163323bea2a780815bc9dc46696a43c00e658c"}}}
```
</details>

### Processed transaction by hash

Retrieve information about a transaction that was processed, given its transaction hash.
The path URL is `<HOST:PORT>/transaction/expired/version1/<transaction-hash>`. Enter a valid transaction hash.

Example:

```sh
curl -s http://127.0.0.1:18888/transaction/processed/version1/8204af872d7d19ef8da947bce67c7a55449bc4e2aa12d2756e9ec7472b4854f7
```

<details> 
<summary><b>Sample output</b></summary>

```json
{"transaction_hash":{"Version1":"29cdf4ccfade736e191bd94835b8560d623b0bcf1a933a183ae484d7924c20ad"},"initiator_addr":{"PublicKey":"0119dfb1d2c12464158a6c2842ab0ea4ebc7723421b22d83dd626b5dfc7b95835c"},"timestamp":"2020-08-07T01:30:42.019Z","ttl":"17h 54m 57s 382ms","block_hash":"5a1e6c4cfba0173e2ffbdb6e694554770f8f60c277b87ef3eb97cac2b9521d83","execution_result":{"Version1":{"Success":{"effect":{"operations":[{"key":"17644600125096963714","kind":"NoOp"},{"key":"13459827733103253581","kind":"Read"},{"key":"11676014375412053969","kind":"Read"},{"key":"9909232825903509900","kind":"Read"},{"key":"8850104445275773933","kind":"Add"}],"transforms":[{"key":"2531221168812666934","transform":{"AddUInt128":"3115144695416809598"}},{"key":"1392271867216378917","transform":"WriteContract"},{"key":"16280628745773001665","transform":{"AddUInt512":"8249938852511436756"}}]},"transfers":["transfer-93b2d942db077f0659f63c0073b8c5cfc42f418e07c5da559cb6474fa7655123","transfer-d91deab111799e0b6fc2c1c8509b80aa2e78823605b11ce56b4177a7ab29a0de","transfer-4eaa442f898aa44df25ab9b52b9f09177c170b43b0f68015c307a7cf004d772a","transfer-73616d87fe918b059d673c7da9dca13c883894f4ff0bab1ffb9db825175e3cc1","transfer-f7472a12eeeaa23adf0cf5ca2329cc64a87b35bd478ac0d3c5774ef309fb4c49"],"cost":"6115103606978039045"}}},"messages":[{"entity_addr":{"SmartContract":[96,208,170,249,191,53,191,48,11,3,51,170,76,50,48,255,137,130,50,209,124,138,205,61,75,151,239,3,242,196,126,127]},"message":{"String":"KXpjKX96KMEDRqOnSHyivAF1sATg2RorsXp2CC7P69kM5wxXlTD83bM0zIv6X44U"},"topic_name":"rcMtmYrZOKhJATCXSN7Z57BUNW1UPzF0","topic_name_hash":"2e58fa22f0d51c7c886c3114510ba577b4a413c89aa044de55d972a2600450ac","topic_index":475963101,"block_index":16528668961632653036},{"entity_addr":{"System":[233,58,15,34,92,205,78,176,36,51,210,212,114,33,41,29,40,75,197,219,12,183,180,32,102,174,222,29,101,7,56,7]},"message":{"String":"fzagGCeHuPXnvMrn1I64kq4RPwcMLW2tOiBsmD1tUmIIz5Dgr9cAokY2KuDPVGMM"},"topic_name":"tsI4hSjHroXRXdim8IBZ3Gd1oOHitCE1","topic_name_hash":"0cebb0111bbe91d29d57ec175d011112362a73af58e7ddf6844609ab0d81ef3c","topic_index":152649425,"block_index":9888272225071285086}]}
```

</details>
<br></br>

### Faults by public key

Retrieve the faults associated with a validator's public key.
The path URL is `<HOST:PORT>/faults/<public-key>`. Enter a valid hexadecimal representation of a validator's public key.

Example:

```sh
curl -s http://127.0.0.1:18888/faults/01a601840126a0363a6048bfcbb0492ab5a313a1a19dc4c695650d8f3b51302703
```

### Faults by era

Return the faults associated with an era, given a valid era identifier.
The path URL is: `<HOST:PORT>/faults/<era-ID>`. Enter an era identifier.

Example:

```sh
curl -s http://127.0.0.1:18888/faults/2304
```

### Finality signatures by block

Retrieve the finality signatures in a block, given its block hash. 

The path URL is: `<HOST:PORT>/signatures/<block-hash>`. Enter a valid block hash.

Example:

```sh
curl -s http://127.0.0.1:18888/signatures/85aa2a939bc3a4afc6d953c965bab333bb5e53185b96bb07b52c295164046da2
```

### Step by era

Retrieve the step event emitted at the end of an era, given a valid era identifier.

The path URL is: `<HOST:PORT>/step/<era-ID>`. Enter a valid era identifier.

Example:

```sh
curl -s http://127.0.0.1:18888/step/7268
```

### Missing filter

If no filter URL was specified after the root address (HOST:PORT), an error message will be returned.

Example:

```sh
curl http://127.0.0.1:18888
{"code":400,"message":"Invalid request path provided"}
```

### Invalid filter

If an invalid filter was specified, an error message will be returned.

Example:

```sh
curl http://127.0.0.1:18888/other
{"code":400,"message":"Invalid request path provided"}
```
