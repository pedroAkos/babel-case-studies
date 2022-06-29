# MyPaxos

My multi-paxos service implement :-)

## Illustrate

This is an implementation of the multi-paxos protocol service, and provides a simple interface that users can expand, as
well as a simple client. This implementation has the following characteristics:

* It is realized that when the same leader submits consecutively in multi-paxos, the protocol process is optimized, and
  the prepare and accept processes are optimized to only the accept process.
* The mechanism of node crash recovery is realized.
* Provides a simple and easy-to-use extension interface for users, based on which users can implement a system based on
  paxos services.
* Provides a grouping function, multiple virtual groups can be set on a single node, and each group is logically
  independent.

## A simple description of the paxos protocol

The problem solved by the Paxos algorithm is how to reach agreement on a certain value in a distributed system where the
above-mentioned abnormalities may occur, so as to ensure that no matter any of the above-mentioned abnormalities occurs,
the consistency of the resolution will not be destroyed. A typical scenario is that in a distributed database system, if
the initial state of each node is the same, and each node performs the same sequence of operations, then they can
finally get a consistent state. In order to ensure that each node executes the same command sequence, a "consistency
algorithm" needs to be executed on each instruction to ensure that the instructions seen by each node are consistent.

There are three roles in the paxos protocol:

* proposer: the initiator of the proposal
* accepter: the recipient of the proposal
* learner: the learner of the proposal

The paxos agreement guarantees that in each round of proposals, as long as a certain proposal is accepted by more than
half of the accepters, the current round of proposals will take effect and will not be modified or destroyed. The
specific algorithm description can be found in [Wikipedia](https://zh.wikipedia.org/wiki/Paxos%E7%AE%97%E6%B3%95).

## Schematic diagram of overall structure and process

* paxos server and client

![](http://7xrlnt.com1.z0.glb.clouddn.com/mypaxos.png)

* The submission process of the paxos protocol

![](http://7xrlnt.com1.z0.glb.clouddn.com/mypaxos-2.png)

* Confirmation of multiple instances

![](http://7xrlnt.com1.z0.glb.clouddn.com/mypaxos-3.png)

* learner's learning

![](http://7xrlnt.com1.z0.glb.clouddn.com/mypaxos-4.png)

## use

To use MyPaxos protocol service, the following steps are required:

* Implement the callback function `PaxosCallback` that needs to be executed when the submission is successful
* Modify the configuration file on each node and start the paxos server
* Start the client and execute the submission request

I use MyPaxos here to implement a distributed simple kv storage.

* Configuration file information

```json
{
  "nodes": [
    {
      // Node 1, server port is 33333
      "id": 1,
      "host": "localhost",
      "port": 33333
    },
    {
      "id": 2,
      "host": "localhost",
      "port": 33334
    },
    {
      "id": 3,
      "host": "localhost",
      "port": 33335
    }
  ],
  "myid": 1,
  //The id of this node
  "timeout": 1000,
  //Communication timeout
  "learningInterval": 1000,
  // learning interval of learner
  "dataDir": "./dataDir/",
  // The location of data persistence, used for crash recovery
  "enableDataPersistence": false
  // Whether to enable data persistence
}
```

* Callback function after successful submission

```java
public class KvCallback implements PaxosCallback {
    /**
     * Use map to save key and value mapping
     */
    private Map<String, String> kv = new HashMap<>();
    private Gson gson = new Gson();

    @Override
    public void callback(String msg) {
        /**
         * A total of three actions are provided: get: get put: add delete: delete
         */
        MsgBean bean = gson.fromJson(msg, MsgBean.class);
        switch (bean.getType()) {
            case "get":
                System.out.println(kv.get(bean.getKey()));
                break;
            case "put":
                kv.put(bean.getKey(), bean.getValue());
                System.out.println("ok");
                break;
            case "delete":
                kv.remove(bean.getKey());
                System.out.println("ok");
                break;
            default:
                break;
        }
    }

}
```

* Message format

```java
public class MsgBean {

    private String type;
    private String key;
    private String value;

    public MsgBean(String type, String key, String value) {
        super();
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
```

* Start the client and send the request

```java
public class ClientTest {
    public static void main(String[] args) {
        MyPaxosClient client = new MyPaxosClient("localhost", 33333);
        try {
            // send to group1
            client.submit(new Gson().toJson(new MsgBean("put", "name", "Mike")), 1);
            // send to group2
            client.submit(new Gson().toJson(new MsgBean("put", "name", "Neo")), 2);
            // send to group1
            client.submit(new Gson().toJson(new MsgBean("get", "name", "")), 1);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
```

* result

Node 1

```
paxos server-1 start...
ok
ok
Mike
```

Node 2

```
paxos server-2 start...
ok
ok
Mike
```

Node 3

```
paxos server-3 start...
ok
ok
Mike
```

## References and materials

* Lamport, Leslie. "The part-time parliament." ACM Transactions on Computer Systems (TOCS) 16.2 (1998): 133-169.
* Lamport, Leslie. "Paxos made simple." ACM Sigact News 32.4 (2001): 18-25.
* Primi, Marco. Paxos made code. Diss. University of Lugano, 2009.
* Chandra, Tushar D., Robert Griesemer, and Joshua Redstone. "Paxos made live: an engineering perspective." Proceedings
  of the twenty-sixth annual ACM symposium on Principles of distributed computing. ACM, 2007.
* [WeChat self-developed production-level paxos class library PhxPaxos implementation principle introduction](http://mp.weixin.qq.com/s?__biz=MzI4NDMyNTU2Mw==&mid=2247483695&idx=1&sn=91ea422913fc62579e020e941d1d059e#rd)
* [Paxos theory introduction (1): the derivation and proof of the naive Paxos algorithm theory] (https://zhuanlan.zhihu.com/p/21438357?refer=lynncui)
* [Paxos Theory Introduction (2): Multi-Paxos and Leader](https://zhuanlan.zhihu.com/p/21466932?refer=lynncui)
