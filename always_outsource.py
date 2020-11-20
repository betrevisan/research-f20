import random
import csv

skills_file = '/Users/bernardoetrevisan/Desktop/research-f20/skills.tsv'
workers_file = '/Users/bernardoetrevisan/Desktop/research-f20/workers.tsv'

def main():
    skills = load_skills() # Load skills into a list.
    workers = load_workers() # Load workers into a list.
    stream = random_stream(skills) # Generates the random stream of requests.

    cost_outsource = always_outsource(stream, workers)
    cost_hire = always_hire(stream, workers)

    print("The cost of always outsourcing was $" + str(format(cost_outsource, '.2f')))
    print("The cost of always hiring was $" + str(format(cost_hire, '.2f')))

# Generates a random stream of requests to be used consistently across the different algorithms.
def random_stream(skills):
    stream = [] # List that keeps track of requests.
    p = 5 # Value of p is defined as 5 for now.

    # Initialize the skill to be a random one.
    curr_skill = random_skill(skills)

    # Stream of 1K requests.
    for i in range(1000):
        stream.append(curr_skill)
        # Only change the current skill acording to p.
        if random.random() <= 1/p:
            curr_skill = random_skill(skills)
    
    return stream

# Algorithm that always outsources workers and keeps the cache empty
def always_outsource(stream, workers):
    cost = 0

    # For each request in the stream, outsource
    for r in stream:
        # Find worker with the given skill and add them to the cache.
        cache = find_worker(workers, r)
        # Outsource worker.
        cost = cost + float(cache[4])
        # Leave the cache empty.
        cache = None
    
    return cost

# Algorithm that always hires workers when they are not in the cache
def always_hire(stream, workers):
    cost = 0
    cache = [None, None, None, None, None, None, None, None]

    # For each request in the stream, outsource
    for r in stream:
        # Hire onlye if the worker is not yet in the cache.
        if r != cache[7]:
            # Find worker with the given skill and add them to the cache.
            cache = find_worker(workers, r)
            # Hire worker and keep them in the cache.
            cost = cost + float(cache[3])
    
    return cost

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
    return random.choice(skills)[0]

# Helper function that finds the worker with the given skill id and returns them.
def find_worker(workers, skill_id):
    for worker in workers:
        if worker[7] == skill_id:
            return worker
    return False




if __name__ == "__main__":
    main()