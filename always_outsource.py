import random
import csv

skills_file = '/Users/bernardoetrevisan/Desktop/research-f20/skills.tsv'
workers_file = '/Users/bernardoetrevisan/Desktop/research-f20/workers.tsv'

def main():
    skills = load_skills() # Load skills into a list.
    workers = load_workers() # Load workers into a list.
    print(workers[0])
    p = 5 # Value of p is defined as 5 for now.
    history = [] # List that keeps track of requests

    # Initialize the skill to be a random one.
    curr_skill = random_skill(skills)
    history.append(curr_skill)
    # cache = find_worker()

    # Stream of 1K requests.
    for i in range(1000):
        if random.random() <= 1/p:
            curr_skill = random_skill(skills)
            #cache = find_worker()
        
        history.append(curr_skill)

        # Outsource worker with this given skill

# Helper function that reads the skills tsv file and stores it into an output list.     
def load_skills():
    skills_tsv = open(skills_file)
    skills = list(csv.reader(skills_tsv, delimiter="\t"))
    skills_tsv.close()
    return skills

# Helper function that reads the workers tsv file and stores it into an output list. 
def load_workers():
    workers_tsv = open(workers_file)
    workers = list(csv.reader(workers_tsv, delimiter="\t"))
    workers_tsv.close()
    return workers

# Helper function that returns a random skill from all the possible ones.
def random_skill(skills):
    return random.choice(skills)




if __name__ == "__main__":
    main()