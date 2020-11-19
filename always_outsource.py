import random

def main():
    # Load tsv into a dictionary.

    # Value of p is defined as 5 for now.
    p = 5

    # Initialize the skill to be a random one.
    curr_skill = random_skill()

    # REMINDER: store stream of requests in a list.
    # Stream of 1K requests.
    for i in range(1000):
        curr_skill = random_skill() if random.random() <= 1/p else curr_skill
            

# Helper function that return a random skill from all the possible ones.
def random_skill():



if __name__ == "__main__":
    main()