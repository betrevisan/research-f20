import random
import csv
import matplotlib.pyplot as plt

skills_file = '/Users/bernardoetrevisan/Desktop/research-f20/skills.tsv'
workers_file = '/Users/bernardoetrevisan/Desktop/research-f20/workers.tsv'

def main():
    skills = load_skills() # Load skills into a list.
    workers = load_workers() # Load workers into a list.
    stream = random_stream(skills) # Generates the random stream of requests.

    cache(3, workers, stream)

def cache(size, workers, stream):
    cost_outsource = always_outsource(size, stream, workers)
    cost_hire = always_hire(size, stream, workers)
    cost_lru = lru(size, stream, workers)
    cost_out_greater_hire = out_greater_hire(size, stream, workers)
    cost_primal_dual = primal_dual(size, stream, workers)

    print("The cost of always outsourcing was $" + str(format(cost_outsource[1000], '.2f')))
    print("The cost of always hiring was $" + str(format(cost_hire[1000], '.2f')))
    print("The cost of using the LRU policy was $" + str(format(cost_lru[1000], '.2f')))
    print("The cost of hiring once outsourcing becomes too expensive was $" + str(format(cost_out_greater_hire[1000], '.2f')))
    print("The cost of the primal dual algorithm was $" + str(format(cost_primal_dual[1000], '.2f')))

    # Display plots
    # plt.plot(cost_outsource, label='Outsource')
    # plt.plot(cost_hire, label='Hire')
    # plt.plot(cost_lru, label='LRU')
    # plt.plot(cost_out_greater_hire, label='Outsource >= Hiring')
    # plt.plot(cost_primal_dual, label='Primal-Dual')
    # plt.xlabel("Length of Stream of Requests")
    # plt.ylabel("Total Cost")
    # plt.title("Cache of size " + str(size))
    # plt.legend()
    # plt.show()

# Generates a random stream of requests to be used consistently across the different algorithms.
def random_stream(skills):
    stream = [] # List that keeps track of requests.
    p = 5 # Value of p is defined as 5 for now.

    # Initialize the skill to be a random one.
    curr_skill = random_skill(skills)

    # Stream of 1K requests.
    for i in range(10):
        stream.append(curr_skill)
        # Only change the current skill acording to p.
        if random.random() <= 1/p:
            curr_skill = random_skill(skills)
    
    return stream

# Algorithm that always outsources workers and keeps the cache empty
def always_outsource(size, stream, workers):
    cost = 0
    history = [0]

    # For each request in the stream, outsource
    for r in stream:
        # Find worker with the given skill and add them to the cache.
        cache = find_worker(workers, r)
        # Outsource worker.
        cost = cost + float(cache[4])
        history.append(cost)
        # Leave the cache empty.
        cache = None
    
    return history

# Algorithm that always hires workers when they are not in the cache
def always_hire(size, stream, workers):
    cost = 0
    cache = []
    history = [0]

    # For each request in the stream, hire.
    for r in stream:
        # Hire only if the worker is not yet in the cache.
        if not in_cache(cache, r):
            # Find worker with the given skill.
            new_worker = find_worker(workers, r)
            # Add worker to the cache.
            cache.append(new_worker)
            # If the cache is too large.
            if len(cache) > size:
                # Remove the oldest worker
                cache.pop(0)
            
            cost = cost + float(new_worker[3])
        history.append(cost)
    
    return history

# Algorithm that uses the LRU policy.
def lru(size, stream, workers):
    cost = 0
    cache = []
    history = [0]

    # Add a call tracker to each worker.
    workers = add_counter(workers)
    
    # For each request in the stream, apply the LRU policy.
    for r in stream:
        print(r)
        print(cache)
        if not in_cache(cache, r):
            print("hire")
            # Find worker with the given skill.
            new_worker = find_worker(workers, r)

            # Add worker to the cache.
            cache.append(new_worker)
            # If the cache is too large.
            if len(cache) > size:
                # Remove the LRU worker
                remove_lru(cache)
            
            cost = cost + float(new_worker[3])
            # Increment the number of calls of the new worker
            new_worker[8] += 1

        else:
            worker = find_worker(workers, r)
            # Increment the call counter
            worker[8] += 1

        history.append(cost)

    return history

# Algorithm that hires once total outsourcing cost becomes too large
def out_greater_hire(size, stream, workers):
    cost = 0
    cache = [None, None, None, 0, None, None, None, None, 0]
    history = [0]

    # Reset the counter of each worker.
    workers = reset_counter(workers)
    
    # For each request in the stream, apply the outsource greater than hiring policy.
    for r in stream:
        if r != cache[7]:
            # Find worker with the given skill.
            new_worker = find_worker(workers, r)

            # If the worker's total outsourcing costs become more expensive than hiring, hire.
            if new_worker[8] > float(new_worker[3]):
                cache = new_worker
                cost = cost + float(cache[3])
            else:
                # Outsource the worker if outsourcing is still cheaper
                cost = cost + float(new_worker[4])
                new_worker[8] = new_worker[8] + float(new_worker[4])

        history.append(cost)

    return history

# Algorithm that will hire and outsource based on a primal and a dual.
def primal_dual(size, stream, workers):
    cost = 0
    c = 1 # Constant C, which is the size of the cache.
    cache = [None, None, None, None, None, None, None, None, 0] # Cache of size 1
    history = [0]


    # Reset the counter of each worker, which will now be the hiring variable.
    workers = reset_counter(workers)

    # For each request in the stream, apply the primal-dual algorithm.
    for r in stream:
        # If the worker requested is not hired.
        if r != cache[7]:
            new_worker = find_worker(workers, r)

            # Check if their hiring variable is greater than 1.
            if new_worker[8] >= 1:
                # Hire
                cost = cost + float(new_worker[3])
                # Add worker to the cache
                cache = new_worker
                # Restart hiring variable for future iterations
                new_worker[8] = 0
                # PS: In a scenario with a cache of size 1, there is no need for the distinction as to who should be evicted
                # since there is only one option. However, this should be implemented in the case of a cache greater than 1.
            else:
                # Outsource    
                cost = cost + float(new_worker[4])
                # Update hiring variable
                new_worker[8] = new_worker[8] * (1 + 1/float(new_worker[3])) + 1/(c * float(new_worker[3]))
        
        history.append(cost)
    
    return history
            
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

# Helper function that returns true if the worker is in the cache and false otherwise.
def in_cache(cache, skill_id):
    for worker in cache:
        if worker[7] == skill_id:
            return True
    return False

# Helper function that add as counter to each worker
def add_counter(workers):
    for worker in workers:
        worker = worker.append(0)

    return workers

# Helper function that resets the counter of each worker
def reset_counter(workers):
    for worker in workers:
        worker[8] = 0

    return workers

def remove_lru(cache):
    minimum = 1000
    count = 0
    delete = 0

    for worker in cache:
        if worker[8] < minimum:
            min = worker[8]
            delete = count

    cache.pop(delete)

if __name__ == "__main__":
    main()