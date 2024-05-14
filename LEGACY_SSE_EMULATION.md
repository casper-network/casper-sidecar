# Rationale

The casper node 2.x produces a different set of SSE events than the 1.x ones. Also, 1.x nodes used 3 sse endpoints (`/events/sigs`, `/events/deploys`, `/events/main`), while 2.x node exposes all SSE events under one firehose endpoint (`/events`).

Generally the changes in 2.x regarding SSE are backwards incompatible to some extent. To harness all the details and collect all the data clients should adapt the new SSE API. However if some clients are not ready or have no need to adapt to the new SSE API, they can use the legacy SSE emulation.

SSE emulation is by default turned off, the instruction on how to enable it is in the [main README.md](./README.md) file.

**BEFORE YOU ENABLE LEGACY SSE EMULATION** please consider the following:

- The legacy SSE emulation is a temporary solution and can be removed in a future major release.
- The legacy SSE emulation is not a 1:1 mapping of the 2.x events to 1.x events. Some events will be omitted, some will be transformed, some will be passed as is. More details on the limitations of the emulation are explained below.
- The legacy SSE emulation is an additional drain on resources. It will consume more resources than the "native" 2.x SSE API.

# Premises of legacy SSE emulation

Currently the only possible emulation is the V1 SSE API. Enabling V1 SSE api emulation requires setting `emulate_legacy_sse_apis` to `["V1"]`, like:

```
[sse_server]
(...)
emulate_legacy_sse_apis = ["V1"]
(...)
```

This will expose three additional sse endpoints:

- `/events/sigs` -> publishes `ApiVersion`, `BlockAdded`, `DeployProcessed`, `DeployExpired`, `Fault` and `Shutdown`
- `/events/deploys`-> publishes `ApiVersion`, `TransactionAccepted` and `Shutdown`
- `/events/main` -> publishes `ApiVersion`, `FinalitySignature` and `Shutdown` events

Those endpoints will emit events in the same format as the V1 SSE API of the casper node. There are limitations to what Casper Sidecar can and will do, here is a list of mapping assumptions:

## Translating `ApiVersion` event

Legacy SSE event will be the same

## Translating `BlockAdded` event

- When the 2.x event emits a V1 block it will be unwrapped and passed as a legacy BlockAdded, for instance a 2.x event like this:

  ```json
  {
    "BlockAdded": {
      "block_hash": "d59359690ca5a251b513185da0767f744e77645adec82bb6ff785a89edc7591c",
      "block": {
        "Version1": {
          "hash": "d59359690ca5a251b513185da0767f744e77645adec82bb6ff785a89edc7591c",
          "header": {
            "parent_hash": "90ca56a697f8b1b19cba08c642fd7f04669b8cd49bb9d652fca989f8a9f8bcea",
            "state_root_hash": "9cce223fdbeab41dbbcf0b62f3fd857373131378d51776de26bb9f4fefe1e849",
            "body_hash": "5f37be399c15b2394af48243ce10a62a7d12769dc5f7740b18ad3bf55bde5271",
            "random_bit": true,
            "accumulated_seed": "b3e1930565a80a874a443eaadefa1a340927fb8b347729bbd93e93935a47a9e4",
            "era_end": {
              "era_report": {
                "equivocators": [
                  "0203c9da857cfeccf001ce00720ae2e0d083629858b60ac05dd285ce0edae55f0c8e",
                  "02026fb7b629a2ec0132505cdf036f6ffb946d03a1c9b5da57245af522b842f145be"
                ],
                "rewards": [
                  {
                    "validator": "01235b932586ae5cc3135f7a0dc723185b87e5bd3ae0ac126a92c14468e976ff25",
                    "amount": 129457537
                  }
                ],
                "inactive_validators": []
              },
              "next_era_validator_weights": [
                {
                  "validator": "0198957673ad060503e2ec7d98dc71af6f90ad1f854fe18025e3e7d0d1bbe5e32b",
                  "weight": "1"
                },
                {
                  "validator": "02022d6bc4e3012cc4ae467b5525111cf7ed65883b05a1d924f1e654c64fad3a027c",
                  "weight": "2"
                }
              ]
            },
            "timestamp": "2024-04-25T20:00:35.640Z",
            "era_id": 601701,
            "height": 6017012,
            "protocol_version": "1.0.0"
          },
          "body": {
            "proposer": "0203426736da2554ebf1f8ee1d2ce4ab11b1e33419d7dfc1ce2fe1945faf00bacc9e",
            "deploy_hashes": [
              "06950e4374dc88685634ec30bcddd68e6b46c109ccf6d29e2dfcf5367df75571",
              "27a89dd58e6297a5244342b68b117afe2555131b896ad6ed4321edcd4130ae7b"
            ],
            "transfer_hashes": [
              "3e30b6c1c5dbca9277425846b42dc832cd3d8ce889c38d6bfc8bd95b3e1c403e",
              "c990ba47146270655eaacc53d4115cbd980697f3d4e9c76bccfdfce82af6ce08"
            ]
          }
        }
      }
    }
  }
  ```

  will be translated to 1.x emulated event:

  ```json
  {
    "BlockAdded": {
      "block_hash": "d59359690ca5a251b513185da0767f744e77645adec82bb6ff785a89edc7591c",
      "block": {
        "hash": "d59359690ca5a251b513185da0767f744e77645adec82bb6ff785a89edc7591c",
        "header": {
          "parent_hash": "90ca56a697f8b1b19cba08c642fd7f04669b8cd49bb9d652fca989f8a9f8bcea",
          "state_root_hash": "9cce223fdbeab41dbbcf0b62f3fd857373131378d51776de26bb9f4fefe1e849",
          "body_hash": "5f37be399c15b2394af48243ce10a62a7d12769dc5f7740b18ad3bf55bde5271",
          "random_bit": true,
          "accumulated_seed": "b3e1930565a80a874a443eaadefa1a340927fb8b347729bbd93e93935a47a9e4",
          "era_end": {
            "era_report": {
              "equivocators": [
                "0203c9da857cfeccf001ce00720ae2e0d083629858b60ac05dd285ce0edae55f0c8e",
                "02026fb7b629a2ec0132505cdf036f6ffb946d03a1c9b5da57245af522b842f145be"
              ],
              "rewards": [
                {
                  "validator": "01235b932586ae5cc3135f7a0dc723185b87e5bd3ae0ac126a92c14468e976ff25",
                  "amount": 129457537
                }
              ],
              "inactive_validators": []
            },
            "next_era_validator_weights": [
              {
                "validator": "0198957673ad060503e2ec7d98dc71af6f90ad1f854fe18025e3e7d0d1bbe5e32b",
                "weight": "1"
              },
              {
                "validator": "02022d6bc4e3012cc4ae467b5525111cf7ed65883b05a1d924f1e654c64fad3a027c",
                "weight": "2"
              }
            ]
          },
          "timestamp": "2024-04-25T20:00:35.640Z",
          "era_id": 601701,
          "height": 6017012,
          "protocol_version": "1.0.0"
        },
        "body": {
          "proposer": "0203426736da2554ebf1f8ee1d2ce4ab11b1e33419d7dfc1ce2fe1945faf00bacc9e",
          "deploy_hashes": [
            "06950e4374dc88685634ec30bcddd68e6b46c109ccf6d29e2dfcf5367df75571",
            "27a89dd58e6297a5244342b68b117afe2555131b896ad6ed4321edcd4130ae7b"
          ],
          "transfer_hashes": [
            "3e30b6c1c5dbca9277425846b42dc832cd3d8ce889c38d6bfc8bd95b3e1c403e",
            "c990ba47146270655eaacc53d4115cbd980697f3d4e9c76bccfdfce82af6ce08"
          ]
        }
      }
    }
  }
  ```

- When the 2.x event emits a V2 block the following rules apply:

  - `block_hash` will be copied from V2 to V1
  - `block.block_hash` will be copied from V2 to V1
  - `block.header.era_end`:
    - if the era_end is a V1 variety - it will be copied
    - if the era_end is a V2 variety:
      - V2 `next_era_validator_weights` will be copied from V2 `next_era_validator_weights`
      - V1 `era_report` will be assembled from V2 `era_end.equivocators`, `era_end.rewards` and `era_end.inactive_validators` fields
      - IF one of the `rewards` contains a reward that doesn't fit in a u64 (because V2 has U512 type in rewards values) - the whole `era_end` **WILL BE OMITTED** from the legacy V1 block (value None)
      - V2 field `next_era_gas_price` has no equivalent in V1 and will be omitted
  - `block.header.current_gas_price` this field only exists in V2 and will be omitted from the V1 block header
  - `block.header.proposer` will be copied from V2 to V1 `block.body.proposer`
  - other `block.header.*` fields will be copied from V2 to V1
  - `block.body.deploy_hashes` will be based on V2 `block.body.standard` transactions. Bear in mind, that only values of transactions of type `Deploy` will be copied to V1 `block.body.deploy_hashes` array
  - `block.body.transfer_hashes` will be based on V2 `block.body.mint` transactions. Bear in mind, that only values of transactions of type `Deploy` will be copied to V1 `block.body.transfer_hashes` array.

  An example of the above rules.
  Input V2 BlockAdded:

  ```json
  {
    "BlockAdded": {
      "block_hash": "2df9fb8909443fba928ed0536a79780cdb4557d0c05fdf762a1fd61141121422",
      "block": {
        "Version2": {
          "hash": "2df9fb8909443fba928ed0536a79780cdb4557d0c05fdf762a1fd61141121422",
          "header": {
            "proposer": "01d3eec0445635f136ae560b43e9d8f656a6ba925f01293eaf2610b39ebe0fc28d",
            "parent_hash": "b8f5e9afd2e54856aa1656f962d07158f0fdf9cfac0f9992875f31f6bf2623a2",
            "state_root_hash": "cbf02d08bb263aa8915507c172b5f590bbddcd68693fb1c71758b5684b011730",
            "body_hash": "6041ab862a1e14a43a8e8a9a42dad27091915a337d18060c22bd3fe7b4f39607",
            "random_bit": false,
            "accumulated_seed": "a0e424710f4fba036ba450b40f2bd7a842b176cf136f3af1952a2a13eb02616c",
            "era_end": {
              "equivocators": [
                "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc54",
                "0203e4532e401326892aa8ebc16b6986bd35a6c96a1f16c28db67fd7e87cb6913817",
                "020318a52d5b2d545def8bf0ee5ea7ddea52f1fbf106c8b69848e40c5460e20c9f62"
              ],
              "inactive_validators": [
                "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc55",
                "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc56"
              ],
              "next_era_validator_weights": [
                {
                  "validator": "02038b238d774c3c4228a0430e3a078e1a2533f9c87cccbcf695637502d8d6057a63",
                  "weight": "1"
                },
                {
                  "validator": "0102ffd4d2812d68c928712edd012fbcad54367bc6c5c254db22cf696772856566",
                  "weight": "2"
                }
              ],
              "rewards": {
                "02028b18c949d849b377988ea5191b39340975db25f8b80f37cc829c9f79dbfb19fc": "749546792",
                "02028002c063228ff4e9d22d69154c499b86a4f7fdbf1d1e20f168b62da537af64c2": "788342677",
                "02038efa405f648c72f36b0e5f37db69ab213d44404591b24de21383d8cc161101ec": "86241635",
                "01f6bbd4a6fd10534290c58edb6090723d481cea444a8e8f70458e5136ea8c733c": "941794198"
              },
              "next_era_gas_price": 1
            },
            "timestamp": "2024-04-25T20:31:39.895Z",
            "era_id": 419571,
            "height": 4195710,
            "protocol_version": "1.0.0",
            "current_gas_price": 1
          },
          "body": {
            "transactions": {
              "0": [{
                "Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e80"
              },
              {
                "Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e81"
              },
              {
                "Version1": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e82"
              }],
              "1": [{
                "Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e83"
              },
              {
                "Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e84"
              },
              {
                "Version1": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e85"
              }],
              "2": [{
                "Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e86"
              },
              {
                "Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e87"
              },
              {
                "Version1": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e88"
              }],
              "3": [{
                "Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e89"
              },
              {
                "Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e90"
              },
              {
                "Version1": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e91"
              }]
            }
            "rewarded_signatures": [[240], [0], [0]]
          }
        }
      }
    }
  }
  ```

  Output legacy BlockAdded:

  ```json
  {
    "BlockAdded": {
      "block_hash": "2df9fb8909443fba928ed0536a79780cdb4557d0c05fdf762a1fd61141121422",
      "block": {
        "hash": "2df9fb8909443fba928ed0536a79780cdb4557d0c05fdf762a1fd61141121422",
        "header": {
          "parent_hash": "b8f5e9afd2e54856aa1656f962d07158f0fdf9cfac0f9992875f31f6bf2623a2",
          "state_root_hash": "cbf02d08bb263aa8915507c172b5f590bbddcd68693fb1c71758b5684b011730",
          "body_hash": "6041ab862a1e14a43a8e8a9a42dad27091915a337d18060c22bd3fe7b4f39607",
          "random_bit": false,
          "accumulated_seed": "a0e424710f4fba036ba450b40f2bd7a842b176cf136f3af1952a2a13eb02616c",
          "era_end": {
            "era_report": {
              "equivocators": [
                "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc54",
                "0203e4532e401326892aa8ebc16b6986bd35a6c96a1f16c28db67fd7e87cb6913817",
                "020318a52d5b2d545def8bf0ee5ea7ddea52f1fbf106c8b69848e40c5460e20c9f62"
              ],
              "rewards": [
                {
                  "validator": "01f6bbd4a6fd10534290c58edb6090723d481cea444a8e8f70458e5136ea8c733c",
                  "amount": 941794198
                },
                {
                  "validator": "02028002c063228ff4e9d22d69154c499b86a4f7fdbf1d1e20f168b62da537af64c2",
                  "amount": 788342677
                },
                {
                  "validator": "02028b18c949d849b377988ea5191b39340975db25f8b80f37cc829c9f79dbfb19fc",
                  "amount": 749546792
                },
                {
                  "validator": "02038efa405f648c72f36b0e5f37db69ab213d44404591b24de21383d8cc161101ec",
                  "amount": 86241635
                }
              ],
              "inactive_validators": [
                "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc55",
                "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc56"
              ]
            },
            "next_era_validator_weights": [
              {
                "validator": "0102ffd4d2812d68c928712edd012fbcad54367bc6c5c254db22cf696772856566",
                "weight": "2"
              },
              {
                "validator": "02038b238d774c3c4228a0430e3a078e1a2533f9c87cccbcf695637502d8d6057a63",
                "weight": "1"
              }
            ]
          },
          "timestamp": "2024-04-25T20:31:39.895Z",
          "era_id": 419571,
          "height": 4195710,
          "protocol_version": "1.0.0"
        },
        "body": {
          "proposer": "01d3eec0445635f136ae560b43e9d8f656a6ba925f01293eaf2610b39ebe0fc28d",
          "deploy_hashes": [
            "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e89",
            "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e90"
          ],
          "transfer_hashes": [
            "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e80",
            "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e81"
          ]
        }
      }
    }
  }
  ```

## Translating `TransactionAccepted` event

- If the event is a V1 variant - it will be unwrapped and passed, so a 2.x event:
  ```json
  {
    "TransactionAccepted": {
      "Deploy": {
        "hash": "5a7709969c210db93d3c21bf49f8bf705d7c75a01609f606d04b0211af171d43",
        "header": {
          "account": "02022c07e061d6e0b43bbaa25717b021c2ddc0f701a223946a0883b57ae842917438",
          "timestamp": "2020-08-07T01:28:27.360Z",
          "ttl": "4m 22s",
          "gas_price": 72,
          "body_hash": "aa2a111c086628a161001160756c5884e32fde0356bb85f484a3e55682ad089f",
          "dependencies": [],
          "chain_name": "casper-example"
        },
        "payment": {
          "StoredContractByName": {
            "name": "casper-example",
            "entry_point": "example-entry-point",
            "args": [
              [
                "amount",
                {
                  "cl_type": "U512",
                  "bytes": "0400f90295",
                  "parsed": "2500000000"
                }
              ]
            ]
          }
        },
        "session": {
          "StoredContractByHash": {
            "hash": "dfb621e7012df48fe1d40fd8015b5e2396c477c9587e996678551148a06d3a89",
            "entry_point": "8sY9fUUCwoiFZmxKo8kj",
            "args": [
              [
                "YbZWtEuL4D6oMTJmUWvj",
                {
                  "cl_type": {
                    "List": "U8"
                  },
                  "bytes": "5a000000909ffe7807b03a5db0c3c183648710db16d408d8425a4e373fc0422a4efed1ab0040bc08786553fcac4521528c9fafca0b0fb86f4c6e9fb9db7a1454dda8ed612c4ea4c9a6378b230ae1e3c236e37d6ebee94339a56cb4be582a",
                  "parsed": [144, 159, 254, 120, 7]
                }
              ]
            ]
          }
        },
        "approvals": [
          {
            "signer": "02022c07e061d6e0b43bbaa25717b021c2ddc0f701a223946a0883b57ae842917438",
            "signature": "025d0a7ba37bebe6774681ca5adecb70fa4eef56821eb344bf0f6867e171a899a87edb2b8bf70f2cb47a1670a6baf2cded1fad535ee53a2f65da91c82ebf30945b"
          }
        ]
      }
    }
  }
  ```
  will be translated to legacy `DeployAccepted`:
  ```json
  {
    "DeployAccepted": {
      "hash": "5a7709969c210db93d3c21bf49f8bf705d7c75a01609f606d04b0211af171d43",
      "header": {
        "account": "02022c07e061d6e0b43bbaa25717b021c2ddc0f701a223946a0883b57ae842917438",
        "timestamp": "2020-08-07T01:28:27.360Z",
        "ttl": "4m 22s",
        "gas_price": 72,
        "body_hash": "aa2a111c086628a161001160756c5884e32fde0356bb85f484a3e55682ad089f",
        "dependencies": [],
        "chain_name": "casper-example"
      },
      "payment": {
        "StoredContractByName": {
          "name": "casper-example",
          "entry_point": "example-entry-point",
          "args": [
            [
              "amount",
              {
                "cl_type": "U512",
                "bytes": "0400f90295",
                "parsed": "2500000000"
              }
            ]
          ]
        }
      },
      "session": {
        "StoredContractByHash": {
          "hash": "dfb621e7012df48fe1d40fd8015b5e2396c477c9587e996678551148a06d3a89",
          "entry_point": "8sY9fUUCwoiFZmxKo8kj",
          "args": [
            [
              "YbZWtEuL4D6oMTJmUWvj",
              {
                "cl_type": {
                  "List": "U8"
                },
                "bytes": "5a000000909ffe7807b03a5db0c3c183648710db16d408d8425a4e373fc0422a4efed1ab0040bc08786553fcac4521528c9fafca0b0fb86f4c6e9fb9db7a1454dda8ed612c4ea4c9a6378b230ae1e3c236e37d6ebee94339a56cb4be582a",
                "parsed": [144, 159, 254, 120, 7]
              }
            ]
          ]
        }
      },
      "approvals": [
        {
          "signer": "02022c07e061d6e0b43bbaa25717b021c2ddc0f701a223946a0883b57ae842917438",
          "signature": "025d0a7ba37bebe6774681ca5adecb70fa4eef56821eb344bf0f6867e171a899a87edb2b8bf70f2cb47a1670a6baf2cded1fad535ee53a2f65da91c82ebf30945b"
        }
      ]
    }
  }
  ```

* If the event is a V2 variant - it will be omitted so a 2.x event like:
  ```
  {
      "TransactionAccepted": {
          "Version1": {
              ...
          }
      }
  }
  ```
  will be omitted from the legacy SSE streams

## Translating `TransactionExpired` event

- If it's a Deploy variety it will be unpacked and sent. So a 2.x `TransactionExpired` event:

  ```json
  {
    "TransactionExpired": {
      "transaction_hash": {
        "Deploy": "565d7147e28be402c34208a133fd59fde7ac785ae5f0298cb5fb7adfb1b054a8"
      }
    }
  }
  ```

  will be sent as a legacy `DeployExpired` event:

  ```json
  {
    "DeployExpired": {
      "deploy_hash": "565d7147e28be402c34208a133fd59fde7ac785ae5f0298cb5fb7adfb1b054a8"
    }
  }
  ```

* If it's a Version1 variant it will be omitted from legacy SSE streams. So a 2.x `TransactionExpired` event:

  ```json
  {
    "TransactionExpired": {
      "Version1": {
        "hash": "565d7147e28be402c34208a133fd59fde7ac785ae5f0298cb5fb7adfb1b054a8"
      }
    }
  }
  ```

  will be omitted

## Translating `TransactionProcessed` event.

- If `transaction_hash` field is a `Version1`, the event will be ignored.
- If `transaction_hash` field is a `Deploy`, it's value will be used as `DeployProcessed.deploy_hash`
  - If `initiator_addr` field is not a `PublicKey` type, the event will be omitted.
  - If `initiator_addr` field is a `PublicKey` type, it's value will be used as `DeployProcessed.account`
  - `timestamp`, `ttl`, `block_hash` will be filled from analogous fields in the `TransactionProcessed` event
  - If `execution_result` is a `Version1` type, it's value will be copied as-is do the `DeployProcessed.execution_result` field.
  - If `execution_result` is a `Version2` type please see [this paragraph](#translating-executionresultv2)

### Translating `ExecutionResultV2`.

- When translating `ExecutionResultV2` (later in this paragraph called `ex_v2`) to legacy `ExecutionResult` (later in this paragraph called `ex_v1`) the following rules apply:
  - if `ex_v2.error_message` is not empty, the `ExecutionResult` will be of type `Failure` and `ex_v1.error_message` will be set to that value. Otherwise `ex_v1` will be of type `Success`
  - `ex_v1.cost` will be set to `ex_v2.cost`
  - `ex_v1.transfers` will always be an empty list since 2.x node doesn't use a notion of `TransferAddr` anymore
  - `ex_v1.effect` will be populated based on `ex_v2.effects` field applying rules from paragraph [Translating Effects from V2](#translating-effects-from-v2)

### Translating `Effects` from V2

- Output `operations` field will always be an empty list, since 2.x node no longer uses this concept for execution results
- For `transforms` the objects will be constructed based on `ex_v2.effects` with the following exceptions:
  - V2 `AddKeys` transform will be translated to V1 `NamedKeys` transform.
  - V2 `Write` transform will be translated applying rules from paragraph [Translating Write transform from V2](#translating-write-transform-from-v2). If translating at least one `Write` transform is not translatable (In the paragraph it will be denoted that it yields a `None` value) - the whole transform will be an empty array.

### Translating `Write` transform from V2

- When translating `Write` transforms from V2 to V1 the following rules apply:
  - For `CLValue`, it will be copied to output as `WriteCLValue` transform
  - For `Account` it will be copied to output as `WriteAccount` transform, taking the v2 `account_hash` as value for `WriteAccount`.
  - For `ContractWasm` a `WriteContractWasm` transform will be created. Please note that `WriteContractWasm` has no data, so details from V2 will be omitted.
  - For `Contract` a `WriteContract` transform will be created. Please note that `WriteContract` has no data, so details from V2 will be omitted.
  - For `Contract` a `WriteContractPackage` transform will be created. Please note that `WriteContractPackage` has no data, so details from V2 will be omitted.
  - For `LegacyTransfer` a `WriteTransfer` transform will be created. Data will be copied.
  - For `DeployInfo` a `WriteDeployInfo` transform will be created. Data will be copied.
  - For `EraInfo` a `ErInfo` transform will be created. Data will be copied.
  - For `Bid` a `WriteBid` transform will be created. Data will be copied.
  - For `Withdraw` a `WriteWithdraw` transform will be created. Data will be copied.
  - For `NamedKey` will be translated into a `AddKeys` transform. Data will be copied.
  - For `AddressableEntity` no value will be produced (a `None` value will be yielded).
  - For `BidKind` no value will be produced (a `None` value will be yielded).
  - For `Package` no value will be produced (a `None` value will be yielded).
  - For `ByteCode` no value will be produced (a `None` value will be yielded).
  - For `MessageTopic` no value will be produced (a `None` value will be yielded).
  - For `Message` no value will be produced (a `None` value will be yielded).
