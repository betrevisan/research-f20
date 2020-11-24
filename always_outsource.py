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
    cost_lru = lru(stream, workers)

    print("The cost of always outsourcing was $" + str(format(cost_outsource, '.2f')))
    print("The cost of always hiring was $" + str(format(cost_hire, '.2f')))
    print("The cost of using the LRU policy was $" + str(format(cost_lru, '.2f')))

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

    # For each request in the stream, hire.
    for r in stream:
        # Hire onlye if the worker is not yet in the cache.
        if r != cache[7]:
            # Find worker with the given skill and add them to the cache.
            cache = find_worker(workers, r)
            # Hire worker and keep them in the cache.
            cost = cost + float(cache[3])
    
    return cost


def lru(stream, workers):
    cost = 0
    cache = [None, None, None, None, None, None, None, None, 0]

    # Add a call tracker to each worker.
    workers = add_counter(workers)
    
    # For each request in the stream, apply the LRU policy.
    for r in stream:
        if r != cache[7]:
            # Find worker with the given skill.
            new_worker = find_worker(workers, r)

            # If the worker with the given skill occurs more often than the one in the cache, hire.
            if cache[8] <= new_worker[8]:
                cache = new_worker
                cost = cost + float(cache[3])
                # Increment the call counter
                cache[8] += 1
            else:
                # Outsource the worker if the one in the cache is more popular
                cost = cost + float(new_worker[4])
                new_worker[8] += 1
        else:
            # Increment the call counter
            cache[8] += 1

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

# Helper function that add as counter to each worker
def add_counter(workers):
    for worker in workers:
        worker = worker.append(0)

    return workers

if __name__ == "__main__":
    main()