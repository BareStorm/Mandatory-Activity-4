# Distribtued Mutual Exclusion with Ricart-Agrawala algorithm
This project implements dustributed mutual exclusion between a set of peer nodes by using the Ricart-Argrawala algorithm. The system doesn't have a central coordinator and by that, every node is equal and the nodes agree among themselves on who may enter the Critical Section.

The Critical Section is emulated when a node is inside it. The node will print:
```
>>> node <id> is in the CRITICAL SECTION <<<
```
And will in there for 2 seconds before leaving.

## Running the project
Clone or download the repository. Then open 3 seperate terminals, all in the project folder and then run one command in each of them. Start them in any order. 

Terminal 1:
```
go run . -id 1 -addr localhost:5001 -peers 2=localhost:5002,3=localhost:5003
```

Terminal 2:
```
go run . -id 2 -addr localhost:5002 -peers 1=localhost:5001,3=localhost:5003
```

Terminal 3:

```
go run . -id 3 -addr localhost:5003 -peers 1=localhost:5001,2=localhost:5002
```
In each terminal you should start to see the node listing, register its two peers, and with for input.

### Request the Critical Section
Click into a node's terminal and press Enter. That node asks the others for permission, and after both reply it enter the Critical Section and prints:

```
>>> node 1 is in the CRITICAL SECTION <<<
```
It will stay there for 2 seconds and then exits.

To see a conflict, press Enter in two or three terminals at almost the same time. One node enters first, and the others log DEFERRING and enter one after another when the node ahead of them exits.

### Automatic requests
Add -auto to a node's command and it requests the Critical Section by itself at random in a intervals of 1-4 seconds. Use it on all three nodes for a continuous demo without pressing Enter.
```
go run . -id 1 -addr localhost:5001 -peers 2=localhost:5002,3=localhost:5003 -auto
```
And to stop the nodes, press Ctrl+C in each terminals.
