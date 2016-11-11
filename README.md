Insight Data Engineering Coding Challenge:

The code adds features to a digital wallet company called PayMo. The features involve checking whether the corresponding  users fall within a certain neighbourboud of each other with regard to the existing transaction history:

- The first feature checks whether the users have ever made any transaction.
- The second feature checks whether the users are friends or friends of friends.
- The third feature checks whether the users fall within the 4th-degree friends network.

The code is implemented in Python and resides in PaymoDigitalFraud/src folder together with the corresponding unit test file. To run the code simply call the run.sh script using the following command:

PaymoDigitalFraud$ ./run.sh

The code uses paymo_input/batch_payment.txt file to build the initial state of the graph. The outputs of the code file will reside in the paymo_output folder. Each output file corresponds to testing the relevant feature against paymo_input/stream_payment.txt file.      
