# Linda Tuple Space
Submitted by Sen Zhang

## System Overview
<img src='https://raw.githubusercontent.com/jsongcat/linda_tuple_space/master/viz/linda_tuple_space_overview.png' title='Linda tuple space Overview' width='' alt='Linda tuple space Overview' />
This image was created using online mockup tool <a href="https://moqups.com">Moqups</a>.

Linda Tuple Space is a distributed P2P system to provide a conceptually “global” tuple space which the remote process can access the matched tuples in tuple space.

## System Features
- Concurrency
- Scalability
- Fault tolerance
- Transparency

## How do I use Linda Tuple Space?
### For users
- Write the tuple into the tuple space
```
linda> out (<item> [<item>, <item> ...])
```
- Read the tuple from the tuple space
```
linda> rd (<item> [<item>, <item> ...])
```
- Read the tuple with type match from the tuple space
```
linda> rd (<?<var>:<type>> [<item>, <item> ...])
```
- Delete the tuple from the tuple space
```
linda> in (<item> [<item>, <item> ...])
```
- Delete the tuple with type match from the tuple space
```
linda> in (<?<var>:<type>> [<item>, <item> ...])
```

### For administrators
- Bootup/reboot the node
```
$ java P2 <hostName>
```
- Add the nodes into the tuple space
```
linda> add (<hostName>, <iPAddress>, <portNum>) [(<hostName>, <iPAddress>, <portNum>) ...]
```
- Delete the nodes from the tuple space
```
linda> delete (<hostName> [, <hostName>, <hostName>])
```

## License
Copyright 2017 Sen Zhang
