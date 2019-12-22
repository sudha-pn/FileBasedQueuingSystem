* Data is pickled over a file, stored in a directory and given a sequential name.

* The sequential name is milliseconds since epoch plus a random number that should prevent clashes. 

* Lock Mechanism implemantation:
  * The *.lock files are ignored when popping from the queue, so a file will be locked (.lock suffix added) when being written and locked also when being read, and then unlinked.
