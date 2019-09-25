## Danish Farid - 20837628

## CS651 - Fall 2019 - U Waterloo

### Question 1 

    I have only done the pairs implementation. 

    Pairs:
        * I have used 2 jobs
        * JOB 1 Computes line counts and word counts (total count is counted by exporting a "*" for every line)
        * JOB 2 Reads counts and int data, and computes pair counts -> by exporting pair of strings by running a second loop and creating pair.
        * Each reducer reads the int data from an int file including the total count

### Question 2

    Time: ~25 seconds without combiners (only the map reduce job running)

### Question 3

    Time: ~31.5 seconds with combiners (only the map reduce job running)

### Question 4

    77198 pairs

### Question 5

    Highest PMI: 

        (maine, anjou): (3.6331422, 12)
        (anjou, maine): (3.6331422, 12)

    Lowest PMI: 

        (thy, you): (-1.5303968, 11)
        (you, thy): (-1.5303968, 11)

### Question 6

    Tears

        (tears, shed) (2.1117902, 15)
        (tears, salt) (2.052812, 11)
        (tears, eyes) (1.165167, 23)

    death

        (death, father's) (1.120252, 21)
        (death, die) (0.7541594, 18)
        (death, life) (0.73813456, 31)




