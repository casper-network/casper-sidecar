# Casper Sidecar USAGE

This document describes how to consume events and perform queries using the Sidecar, covering the following topics:

- Node-generated events emitted by the node(s) to which the Sidecar connects
- Sidecar-generated events originating solely from the Sidecar service and not from a node
- The RESTful endpoint for performing useful queries about the state of the network

## Prerequisites

- Run the service as described in the [README](README.md).

## The Sidecar Event Stream

The Sidecar event stream is a passthrough for all the events emitted by the node(s) to which the Sidecar connects. This stream also includes one endpoint for Sidecar-generated events that can be useful, although the node did not emit them.

Events are emitted on two endpoints:

- All events that come from a node are re-emitted under `http://<HOST>:<SIDECAR_SSE_PORT>/events`.
- All Sidecar-generated events reporting the Sidecar's internal state are emitted under `http://<HOST>:<SIDECAR_SSE_PORT>/events/sidecar`.

For more information on various event types emitted by the node, visit the [Monitoring and Consuming Events](https://docs.casper.network/developers/dapps/monitor-and-consume-events/#event-types) documentation.

### Monitoring the Sidecar event stream

It is possible to monitor the Sidecar event stream using _cURL_, depending on how the HOST and PORT are configured.

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

data:{ "TransactionProcessed": { "transaction_hash": { "Version1": "25329c14a4f9307830f2b4b6b529b0c3fd618dec65af7456ad9f9e2c7ba1ff4a" }, "initiator_addr": { "PublicKey": "02024e2b994a52bcf4c0cc112512c4be04853c4e824203a8e627c107a8d595707801" }, "timestamp": "2020-08-07T01:30:33.119Z", "ttl": "54m 11s 767ms", "block_hash": "315210f005e7d2d7130004f0178c29cf7e4718d8b642f3f832a35a028ed094cf", "execution_result": { "Version1": { "Success": { "effect": { "operations": [], "transforms": [ { "key": "12730438218135504636", "transform": { "AddUInt256": "16420226327505839383" } }, { "key": "10696215255214620472", "transform": { "AddUInt256": "14018730981435988852" } }, { "key": "15638241704090226222", "transform": { "AddUInt256": "2486508393436959391" } } ] }, "transfers": [], "cost": "2379796918402242989" } } }, "messages": [ { "entity_addr": "entity-contract-a8648307789543cbf38afb24c970844e755654d462a25624edd775219d0cdacf", "message": { "String": "Sax8BEJtXE6vRPXMqOruOhyDxar7N70OeiyPVtfYqiOVNzvHThJwennWwoOs3HHd" }, "topic_name": "PTgw4HZ6CPRhYmSSBbXsI0rnMOcQXgrr", "topic_name_hash": "54a3c9afacf3d475ed69af9de5d4f5496798af12d914aa7f5f8b5cec9935096f", "topic_index": 4003932854, "block_index": 2261021254199878090 } ] }}
id:21821471

:

:

:
```

> **Note**: The Sidecar can connect simultaneously to nodes with different build versions, which send messages with different API versions. There is also the rare possibility of nodes changing API versions and not being in sync with other connected nodes. Although this situation would be rare, clients should be able to parse messages with different API versions.

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

data:{ "BlockAdded": { "block_hash": "9571b9b27dacbed06e048cb656829128e4cab06a45ffe84a5ffff88f919f99b1", "block": { "Version1": { "hash": "4fb7be0031f4dc0d107061065c603d79d3691c37f769e9e3285c73357ae952fa", "header": { "parent_hash": "50fdb9b02e429283ef1ff94c5317185a081eaf56a163f4de0f581eefe999e7b7", "state_root_hash": "ec741f31e84de97db9a1f8d3ed1c48f5448656970a59f9b2267430124ab93fb1", "body_hash": "7a38b4cf9fb5b1ad88724e67a16ae92fc1b76f8647b8aab0585c683ba3008a2f", "random_bit": true, "accumulated_seed": "f6167f91fe62d37fb601cb17dd7ec822f49a31c3da10ff8def194a8d118c4389", "era_end": null, "timestamp": "2025-04-28T10:12:51.985Z", "era_id": 100, "height": 555, "protocol_version": "2.0.0" }, "body": { "proposer": "02027ee0d7fdab27eeab089f292d64d78af865c8e79800f9b376e6ce68a3d5f41f5f", "deploy_hashes": [ "040dd42480760133c25ca33b394395c115090f40577c4792cc3c58c5c7c812fb", "3f9f0f42b4e548693598e7b0ecd969ae7851087c66e6c9733161ad137d75fddf", "8dadb644ca8d6e5678a619c14c7994b832a2d5cfbc222f0ddae6f18ccc61f3f6" ], "transfer_hashes": [] } } } }}
id:1

:

data:"Shutdown"
id:2

:

:

:

data:{"ApiVersion":"2.0.1"}

data:{ "BlockAdded": { "block_hash": "9571b9b27dacbed06e048cb656829128e4cab06a45ffe84a5ffff88f919f99b1", "block": { "Version1": { "hash": "4fb7be0031f4dc0d107061065c603d79d3691c37f769e9e3285c73357ae952fa", "header": { "parent_hash": "50fdb9b02e429283ef1ff94c5317185a081eaf56a163f4de0f581eefe999e7b7", "state_root_hash": "ec741f31e84de97db9a1f8d3ed1c48f5448656970a59f9b2267430124ab93fb1", "body_hash": "7a38b4cf9fb5b1ad88724e67a16ae92fc1b76f8647b8aab0585c683ba3008a2f", "random_bit": true, "accumulated_seed": "f6167f91fe62d37fb601cb17dd7ec822f49a31c3da10ff8def194a8d118c4389", "era_end": null, "timestamp": "2025-04-28T10:12:51.985Z", "era_id": 100, "height": 555, "protocol_version": "2.0.0" }, "body": { "proposer": "02027ee0d7fdab27eeab089f292d64d78af865c8e79800f9b376e6ce68a3d5f41f5f", "deploy_hashes": [ "040dd42480760133c25ca33b394395c115090f40577c4792cc3c58c5c7c812fb", "3f9f0f42b4e548693598e7b0ecd969ae7851087c66e6c9733161ad137d75fddf", "8dadb644ca8d6e5678a619c14c7994b832a2d5cfbc222f0ddae6f18ccc61f3f6" ], "transfer_hashes": [] } } } }}
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
{
  "header": { "api_version": "2.0.0", "network_name": "some-network" },
  "payload": {
    "BlockAdded": {
      "block_hash": "290eb1ecd5c4e8bda94dae647fb9c21aeb531fe817467abb60f7c12be6a672eb",
      "block": {
        "Version2": {
          "hash": "290eb1ecd5c4e8bda94dae647fb9c21aeb531fe817467abb60f7c12be6a672eb",
          "header": {
            "parent_hash": "3236c683ec5f220c1936a25a0be96b976cfbe784f06d2319933b432f2a1fe1eb",
            "state_root_hash": "89ba71746096011be36cda29fdf1d1bd8067af51a0ea7eaf65e90666a35bcbf6",
            "body_hash": "11b6c10321aea27c2fc4d292f570a93b32488759c16cf9ee22e747c35c3873fc",
            "random_bit": true,
            "accumulated_seed": "e376199ca38015a57760e9431fc6723e9500ab3f18c93a26830b5b4ccc9f6a29",
            "era_end": null,
            "timestamp": "2025-04-28T10:12:52.090Z",
            "era_id": 246749,
            "height": 2467498,
            "protocol_version": "1.0.0",
            "proposer": "0203c5ecdb1ad56b65cbc7dbbf99ea492e7566a6b2259191f9ab604c58b19d2a01f3",
            "current_gas_price": 1,
            "last_switch_block_hash": "0808080808080808080808080808080808080808080808080808080808080808"
          },
          "body": {
            "transactions": {
              "0": [],
              "1": [
                {
                  "Version1": "1f3b12822cfa6ef26d8f1e369ffbab37fa0e963385d124db5ab09ba22d2ec452"
                },
                {
                  "Version1": "a13c6a737a926562a02e88f62bf84c3811f2fe20bcc6a9b1454802640dfc730d"
                }
              ],
              "2": [],
              "3": [
                {
                  "Deploy": "5825b4fcdb6e180bd80f83d743910b16e6217dd4e74d1147ac0eb656214ab5d4"
                },
                {
                  "Deploy": "e83a459beef99015de50e3c33ba0acbe658aea8f0b72f0f55599889dc3025b68"
                }
              ],
              "4": [],
              "5": []
            },
            "rewarded_signatures": []
          }
        }
      }
    }
  }
}
```

</details>
<br></br>

### Block by hash

Retrieve information about a block given its block hash.

The path URL is `<HOST:PORT>/block/<block-hash>`. Enter a valid block hash.

Example:

```sh
curl -s http://127.0.0.1:18888/block/290eb1ecd5c4e8bda94dae647fb9c21aeb531fe817467abb60f7c12be6a672eb
```

<details> 
<summary><b>Sample output</b></summary>

```json
{
  "header": { "api_version": "2.0.0", "network_name": "some-network" },
  "payload": {
    "BlockAdded": {
      "block_hash": "290eb1ecd5c4e8bda94dae647fb9c21aeb531fe817467abb60f7c12be6a672eb",
      "block": {
        "Version2": {
          "hash": "290eb1ecd5c4e8bda94dae647fb9c21aeb531fe817467abb60f7c12be6a672eb",
          "header": {
            "parent_hash": "3236c683ec5f220c1936a25a0be96b976cfbe784f06d2319933b432f2a1fe1eb",
            "state_root_hash": "89ba71746096011be36cda29fdf1d1bd8067af51a0ea7eaf65e90666a35bcbf6",
            "body_hash": "11b6c10321aea27c2fc4d292f570a93b32488759c16cf9ee22e747c35c3873fc",
            "random_bit": true,
            "accumulated_seed": "e376199ca38015a57760e9431fc6723e9500ab3f18c93a26830b5b4ccc9f6a29",
            "era_end": null,
            "timestamp": "2025-04-28T10:12:52.090Z",
            "era_id": 246749,
            "height": 2467498,
            "protocol_version": "1.0.0",
            "proposer": "0203c5ecdb1ad56b65cbc7dbbf99ea492e7566a6b2259191f9ab604c58b19d2a01f3",
            "current_gas_price": 1,
            "last_switch_block_hash": "0808080808080808080808080808080808080808080808080808080808080808"
          },
          "body": {
            "transactions": {
              "0": [],
              "1": [
                {
                  "Version1": "1f3b12822cfa6ef26d8f1e369ffbab37fa0e963385d124db5ab09ba22d2ec452"
                },
                {
                  "Version1": "a13c6a737a926562a02e88f62bf84c3811f2fe20bcc6a9b1454802640dfc730d"
                }
              ],
              "2": [],
              "3": [
                {
                  "Deploy": "5825b4fcdb6e180bd80f83d743910b16e6217dd4e74d1147ac0eb656214ab5d4"
                },
                {
                  "Deploy": "e83a459beef99015de50e3c33ba0acbe658aea8f0b72f0f55599889dc3025b68"
                }
              ],
              "4": [],
              "5": []
            },
            "rewarded_signatures": []
          }
        }
      }
    }
  }
}
```

</details>
<br></br>

### Block by chain height

Retrieve information about a block, given a specific block height.

The path URL is `<HOST:PORT>/block/<block-height>`. Enter a valid number representing the block height.

Example:

```sh
curl -s http://127.0.0.1:18888/block/3467498
```

<details> 
<summary><b>Sample output</b></summary>

```json
{
  "header": { "api_version": "2.0.0", "network_name": "some-network" },
  "payload": {
    "BlockAdded": {
      "block_hash": "290eb1ecd5c4e8bda94dae647fb9c21aeb531fe817467abb60f7c12be6a672eb",
      "block": {
        "Version2": {
          "hash": "290eb1ecd5c4e8bda94dae647fb9c21aeb531fe817467abb60f7c12be6a672eb",
          "header": {
            "parent_hash": "3236c683ec5f220c1936a25a0be96b976cfbe784f06d2319933b432f2a1fe1eb",
            "state_root_hash": "89ba71746096011be36cda29fdf1d1bd8067af51a0ea7eaf65e90666a35bcbf6",
            "body_hash": "11b6c10321aea27c2fc4d292f570a93b32488759c16cf9ee22e747c35c3873fc",
            "random_bit": true,
            "accumulated_seed": "e376199ca38015a57760e9431fc6723e9500ab3f18c93a26830b5b4ccc9f6a29",
            "era_end": null,
            "timestamp": "2025-04-28T10:12:52.090Z",
            "era_id": 246749,
            "height": 3467498,
            "protocol_version": "1.0.0",
            "proposer": "0203c5ecdb1ad56b65cbc7dbbf99ea492e7566a6b2259191f9ab604c58b19d2a01f3",
            "current_gas_price": 1,
            "last_switch_block_hash": "0808080808080808080808080808080808080808080808080808080808080808"
          },
          "body": {
            "transactions": {
              "0": [],
              "1": [
                {
                  "Version1": "1f3b12822cfa6ef26d8f1e369ffbab37fa0e963385d124db5ab09ba22d2ec452"
                },
                {
                  "Version1": "a13c6a737a926562a02e88f62bf84c3811f2fe20bcc6a9b1454802640dfc730d"
                }
              ],
              "2": [],
              "3": [
                {
                  "Deploy": "5825b4fcdb6e180bd80f83d743910b16e6217dd4e74d1147ac0eb656214ab5d4"
                },
                {
                  "Deploy": "e83a459beef99015de50e3c33ba0acbe658aea8f0b72f0f55599889dc3025b68"
                }
              ],
              "4": [],
              "5": []
            },
            "rewarded_signatures": []
          }
        }
      }
    }
  }
}
```

</details>
<br></br>

### Transaction by hash

Retrieve an aggregate of the various states a transaction goes through, given its transaction hash. The endpoint also needs the transaction type as an input (`deploy` or `version1`) The node does not emit this event, but the Sidecar computes it and returns it for the given transaction. This endpoint behaves differently than other endpoints, which return the raw event received from the node.

The path URL is `<HOST:PORT>/transaction/<transaction-type>/<transaction-hash>`. Enter a valid transaction hash.

The output differs depending on the transaction's status, which changes over time as the transaction goes through its [lifecycle](https://docs.casper.network/concepts/design/casper-design/#execution-semantics-phases).

Example:

```sh
curl -s http://127.0.0.1:18888/transaction/version1/3141e85f8075c3a75c2a1abcc79810c07d103ff97c03200ab0d0baf91995fe4a
```

The sample output below is for a transaction that was accepted but has yet to be processed.

<details> 
<summary><b>Transaction accepted but not processed yet</b></summary>

```json
{
  "transaction_hash": "3141e85f8075c3a75c2a1abcc79810c07d103ff97c03200ab0d0baf91995fe4a",
  "transaction_accepted": {
    "header": { "api_version": "2.0.0", "network_name": "some-network" },
    "payload": {
      "Version1": {
        "hash": "942785a412289a5aaffdb01d58ee91478bb0cc6b68646519531f4e859ed80566",
        "payload": {
          "initiator_addr": {
            "PublicKey": "02020707086bf373174af44dd96ff43cf73ee4ed01d5a563c97926d880acfda55476"
          },
          "timestamp": "2020-08-07T01:30:31.750Z",
          "ttl": "1h 56m 52s 389ms",
          "chain_name": "xyz",
          "pricing_mode": {
            "Fixed": {
              "additional_computation_factor": 0,
              "gas_price_tolerance": 5
            }
          },
          "fields": {
            "args": {
              "Named": [
                [
                  "delegator",
                  {
                    "cl_type": "PublicKey",
                    "bytes": "01714f5b526d0ce966b0c7d7a6a7eda7fb5b8b10e8b0b18cfe085e8fe7abc0ab66",
                    "parsed": "01714f5b526d0ce966b0c7d7a6a7eda7fb5b8b10e8b0b18cfe085e8fe7abc0ab66"
                  }
                ],
                [
                  "validator",
                  {
                    "cl_type": "PublicKey",
                    "bytes": "01fb60a66cdb914e7448b48213d247edacadc1b17cb2180fbc432460f2fcce497f",
                    "parsed": "01fb60a66cdb914e7448b48213d247edacadc1b17cb2180fbc432460f2fcce497f"
                  }
                ],
                [
                  "amount",
                  {
                    "cl_type": "U512",
                    "bytes": "088063df0de89d7c06",
                    "parsed": "467422081330406272"
                  }
                ]
              ]
            },
            "entry_point": "Undelegate",
            "scheduling": "Standard",
            "target": "Native"
          }
        },
        "approvals": [
          {
            "signer": "02020707086bf373174af44dd96ff43cf73ee4ed01d5a563c97926d880acfda55476",
            "signature": "02b049620953b7f2b828d8e435d37608e7b4dfe203056016a66228b0ffd933d1861dcbeeceab8cfc457a381ce4763734bf3cd1ecb912033a3c340c657d436ebe50"
          }
        ]
      }
    }
  },
  "transaction_expired": false
}
```

</details>
<br></br>

The next sample output is for a transaction that was accepted and processed.

<details> 
<summary><b>Transaction accepted and processed successfully</b></summary>

```json
{
  "transaction_hash": "3141e85f8075c3a75c2a1abcc79810c07d103ff97c03200ab0d0baf91995fe4a",
  "transaction_accepted": {
    "header": { "api_version": "2.0.0", "network_name": "some-network" },
    "payload": {
      "Version1": {
        "hash": "942785a412289a5aaffdb01d58ee91478bb0cc6b68646519531f4e859ed80566",
        "payload": {
          "initiator_addr": {
            "PublicKey": "02020707086bf373174af44dd96ff43cf73ee4ed01d5a563c97926d880acfda55476"
          },
          "timestamp": "2020-08-07T01:30:31.750Z",
          "ttl": "1h 56m 52s 389ms",
          "chain_name": "xyz",
          "pricing_mode": {
            "Fixed": {
              "additional_computation_factor": 0,
              "gas_price_tolerance": 5
            }
          },
          "fields": {
            "args": {
              "Named": [
                [
                  "delegator",
                  {
                    "cl_type": "PublicKey",
                    "bytes": "01714f5b526d0ce966b0c7d7a6a7eda7fb5b8b10e8b0b18cfe085e8fe7abc0ab66",
                    "parsed": "01714f5b526d0ce966b0c7d7a6a7eda7fb5b8b10e8b0b18cfe085e8fe7abc0ab66"
                  }
                ],
                [
                  "validator",
                  {
                    "cl_type": "PublicKey",
                    "bytes": "01fb60a66cdb914e7448b48213d247edacadc1b17cb2180fbc432460f2fcce497f",
                    "parsed": "01fb60a66cdb914e7448b48213d247edacadc1b17cb2180fbc432460f2fcce497f"
                  }
                ],
                [
                  "amount",
                  {
                    "cl_type": "U512",
                    "bytes": "088063df0de89d7c06",
                    "parsed": "467422081330406272"
                  }
                ]
              ]
            },
            "entry_point": "Undelegate",
            "scheduling": "Standard",
            "target": "Native"
          }
        },
        "approvals": [
          {
            "signer": "02020707086bf373174af44dd96ff43cf73ee4ed01d5a563c97926d880acfda55476",
            "signature": "02b049620953b7f2b828d8e435d37608e7b4dfe203056016a66228b0ffd933d1861dcbeeceab8cfc457a381ce4763734bf3cd1ecb912033a3c340c657d436ebe50"
          }
        ]
      }
    }
  },
  "transaction_processed": {
    "header": { "api_version": "2.0.0", "network_name": "some-network" },
    "payload": {
      "transaction_hash": {
        "Version1": "25329c14a4f9307830f2b4b6b529b0c3fd618dec65af7456ad9f9e2c7ba1ff4a"
      },
      "initiator_addr": {
        "PublicKey": "02024e2b994a52bcf4c0cc112512c4be04853c4e824203a8e627c107a8d595707801"
      },
      "timestamp": "2020-08-07T01:30:33.119Z",
      "ttl": "54m 11s 767ms",
      "block_hash": "315210f005e7d2d7130004f0178c29cf7e4718d8b642f3f832a35a028ed094cf",
      "execution_result": {
        "Version1": {
          "Success": {
            "effect": {
              "operations": [],
              "transforms": [
                {
                  "key": "12730438218135504636",
                  "transform": {
                    "AddUInt256": "16420226327505839383"
                  }
                },
                {
                  "key": "10696215255214620472",
                  "transform": {
                    "AddUInt256": "14018730981435988852"
                  }
                },
                {
                  "key": "15638241704090226222",
                  "transform": {
                    "AddUInt256": "2486508393436959391"
                  }
                }
              ]
            },
            "transfers": [],
            "cost": "2379796918402242989"
          }
        }
      },
      "messages": [
        {
          "entity_addr": "entity-contract-a8648307789543cbf38afb24c970844e755654d462a25624edd775219d0cdacf",
          "message": {
            "String": "Sax8BEJtXE6vRPXMqOruOhyDxar7N70OeiyPVtfYqiOVNzvHThJwennWwoOs3HHd"
          },
          "topic_name": "PTgw4HZ6CPRhYmSSBbXsI0rnMOcQXgrr",
          "topic_name_hash": "54a3c9afacf3d475ed69af9de5d4f5496798af12d914aa7f5f8b5cec9935096f",
          "topic_index": 4003932854,
          "block_index": 2261021254199878090
        }
      ]
    }
  },
  "transaction_expired": false
}
```

</details>
<br></br>

### Accepted transaction by hash

Retrieve information about an accepted transaction, given its transaction hash.

The path URL is `<HOST:PORT>/transaction/accepted/<transaction-type>/<transaction-hash>`. Enter a valid transaction hash.

Example:

```sh
curl -s http://127.0.0.1:18888/transaction/accepted/version1/942785a412289a5aaffdb01d58ee91478bb0cc6b68646519531f4e859ed80566
```

<details> 
<summary><b>Sample output</b></summary>

```json
{
  "header": { "api_version": "2.0.0", "network_name": "some-network" },
  "payload": {
    "Version1": {
      "hash": "942785a412289a5aaffdb01d58ee91478bb0cc6b68646519531f4e859ed80566",
      "payload": {
        "initiator_addr": {
          "PublicKey": "02020707086bf373174af44dd96ff43cf73ee4ed01d5a563c97926d880acfda55476"
        },
        "timestamp": "2020-08-07T01:30:31.750Z",
        "ttl": "1h 56m 52s 389ms",
        "chain_name": "xyz",
        "pricing_mode": {
          "Fixed": {
            "additional_computation_factor": 0,
            "gas_price_tolerance": 5
          }
        },
        "fields": {
          "args": {
            "Named": [
              [
                "delegator",
                {
                  "cl_type": "PublicKey",
                  "bytes": "01714f5b526d0ce966b0c7d7a6a7eda7fb5b8b10e8b0b18cfe085e8fe7abc0ab66",
                  "parsed": "01714f5b526d0ce966b0c7d7a6a7eda7fb5b8b10e8b0b18cfe085e8fe7abc0ab66"
                }
              ],
              [
                "validator",
                {
                  "cl_type": "PublicKey",
                  "bytes": "01fb60a66cdb914e7448b48213d247edacadc1b17cb2180fbc432460f2fcce497f",
                  "parsed": "01fb60a66cdb914e7448b48213d247edacadc1b17cb2180fbc432460f2fcce497f"
                }
              ],
              [
                "amount",
                {
                  "cl_type": "U512",
                  "bytes": "088063df0de89d7c06",
                  "parsed": "467422081330406272"
                }
              ]
            ]
          },
          "entry_point": "Undelegate",
          "scheduling": "Standard",
          "target": "Native"
        }
      },
      "approvals": [
        {
          "signer": "02020707086bf373174af44dd96ff43cf73ee4ed01d5a563c97926d880acfda55476",
          "signature": "02b049620953b7f2b828d8e435d37608e7b4dfe203056016a66228b0ffd933d1861dcbeeceab8cfc457a381ce4763734bf3cd1ecb912033a3c340c657d436ebe50"
        }
      ]
    }
  }
}
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
{
  "header": { "api_version": "2.0.0", "network_name": "some-network" },
  "payload": {
    "transaction_hash": {
      "Version1": "3dcf9cb73977a1163129cb0801163323bea2a780815bc9dc46696a43c00e658c"
    }
  }
}
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
{
  "header": { "api_version": "2.0.0", "network_name": "some-network" },
  "payload": {
    "transaction_hash": {
      "Version1": "29cdf4ccfade736e191bd94835b8560d623b0bcf1a933a183ae484d7924c20ad"
    },
    "initiator_addr": {
      "PublicKey": "0119dfb1d2c12464158a6c2842ab0ea4ebc7723421b22d83dd626b5dfc7b95835c"
    },
    "timestamp": "2020-08-07T01:30:42.019Z",
    "ttl": "17h 54m 57s 382ms",
    "block_hash": "5a1e6c4cfba0173e2ffbdb6e694554770f8f60c277b87ef3eb97cac2b9521d83",
    "execution_result": {
      "Version1": {
        "Success": {
          "effect": {
            "operations": [
              { "key": "17644600125096963714", "kind": "NoOp" },
              { "key": "13459827733103253581", "kind": "Read" },
              { "key": "11676014375412053969", "kind": "Read" },
              { "key": "9909232825903509900", "kind": "Read" },
              { "key": "8850104445275773933", "kind": "Add" }
            ],
            "transforms": [
              {
                "key": "2531221168812666934",
                "transform": { "AddUInt128": "3115144695416809598" }
              },
              { "key": "1392271867216378917", "transform": "WriteContract" },
              {
                "key": "16280628745773001665",
                "transform": { "AddUInt512": "8249938852511436756" }
              }
            ]
          },
          "transfers": [
            "transfer-93b2d942db077f0659f63c0073b8c5cfc42f418e07c5da559cb6474fa7655123",
            "transfer-d91deab111799e0b6fc2c1c8509b80aa2e78823605b11ce56b4177a7ab29a0de",
            "transfer-4eaa442f898aa44df25ab9b52b9f09177c170b43b0f68015c307a7cf004d772a",
            "transfer-73616d87fe918b059d673c7da9dca13c883894f4ff0bab1ffb9db825175e3cc1",
            "transfer-f7472a12eeeaa23adf0cf5ca2329cc64a87b35bd478ac0d3c5774ef309fb4c49"
          ],
          "cost": "6115103606978039045"
        }
      }
    },
    "messages": [
      {
        "entity_addr": {
          "SmartContract": [
            96, 208, 170, 249, 191, 53, 191, 48, 11, 3, 51, 170, 76, 50, 48,
            255, 137, 130, 50, 209, 124, 138, 205, 61, 75, 151, 239, 3, 242,
            196, 126, 127
          ]
        },
        "message": {
          "String": "KXpjKX96KMEDRqOnSHyivAF1sATg2RorsXp2CC7P69kM5wxXlTD83bM0zIv6X44U"
        },
        "topic_name": "rcMtmYrZOKhJATCXSN7Z57BUNW1UPzF0",
        "topic_name_hash": "2e58fa22f0d51c7c886c3114510ba577b4a413c89aa044de55d972a2600450ac",
        "topic_index": 475963101,
        "block_index": 16528668961632653036
      },
      {
        "entity_addr": {
          "System": [
            233, 58, 15, 34, 92, 205, 78, 176, 36, 51, 210, 212, 114, 33, 41,
            29, 40, 75, 197, 219, 12, 183, 180, 32, 102, 174, 222, 29, 101, 7,
            56, 7
          ]
        },
        "message": {
          "String": "fzagGCeHuPXnvMrn1I64kq4RPwcMLW2tOiBsmD1tUmIIz5Dgr9cAokY2KuDPVGMM"
        },
        "topic_name": "tsI4hSjHroXRXdim8IBZ3Gd1oOHitCE1",
        "topic_name_hash": "0cebb0111bbe91d29d57ec175d011112362a73af58e7ddf6844609ab0d81ef3c",
        "topic_index": 152649425,
        "block_index": 9888272225071285086
      }
    ]
  }
}
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
curl -s http://127.0.0.1:18888/step/7268
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