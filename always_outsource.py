import random
import csv

skills_file = '/Users/bernardoetrevisan/Desktop/research-f20/skills.tsv'

def main():
    # Load tsv into a list.
    skills = initialize()

    # Value of p is defined as 5 for now.
    p = 5

    # Initialize the skill to be a random one.
    curr_skill = random_skill(skills)

    # REMINDER: store stream of requests in a list.
    # Stream of 1K requests.
    for i in range(1000):
        curr_skill = random_skill() if random.random() <= 1/p else curr_skill

# Helper function that reads the tsv file and stores it into an output list.     
def initialize():
    skills_tsv = open(skills_file)
    skills = list(csv.reader(skills_tsv, delimiter="\t"))
    skills_tsv.close()
    return skills


# Helper function that returns a random skill from all the possible ones.
def random_skill(skills):
    return random.choice(skills)



if __name__ == "__main__":
    main()