import random
import csv
import matplotlib.pyplot as plt
import pprint as pp

skills_file = '/Users/bernardoetrevisan/Desktop/research-f20/skills.tsv'
workers_file = '/Users/bernardoetrevisan/Desktop/research-f20/workers.tsv'

def main():
    skills = load_skills() # Load skills into a list.
    workers = load_workers() # Load workers into a list.

    stream = random_stream(skills, length=1000, p=5, seed=10) # Generates the random stream of requests.

    cache(10, workers, stream)

def cache(size, workers, stream):
    cost_outsource = always_outsource(stream, workers)
    cost_hire = always_hire(size, stream, workers)
    cost_lru = lru(size, stream, workers)
    cost_popularity = popularity(size, stream, workers)
    cost_out_greater_hire = out_greater_hire(size, stream, workers)
    cost_primal_dual = primal_dual(size, stream, workers)

    print("The cost of always outsourcing was $" + str(format(cost_outsource[1000], '.2f')))
    print("The cost of always hiring was $" + str(format(cost_hire[1000], '.2f')))
    print("The cost of using the LRU policy was $" + str(format(cost_lru[1000], '.2f')))
    print("The cost of using the popularity policy was $" + str(format(cost_popularity[1000], '.2f')))
    print("The cost of hiring once outsourcing becomes too expensive was $" + str(format(cost_out_greater_hire[1000], '.2f')))
    print("The cost of the primal dual algorithm was $" + str(format(cost_primal_dual[1000], '.2f')))

    # Display plots
    plt.plot(cost_outsource, label='Outsource')
    plt.plot(cost_hire, label='Hire')
    plt.plot(cost_lru, label='LRU')
    plt.plot(cost_popularity, label='Popularity')
    plt.plot(cost_out_greater_hire, label='Outsource >= Hiring')
    plt.plot(cost_primal_dual, label='Primal-Dual')
    plt.xlabel("Length of Stream of Requests")
    plt.ylabel("Total Cost")
    plt.legend()
    plt.show()

# Generates a random stream of requests to be used consistently across the different algorithms.
def random_stream(skills, length=1000, p=5, seed=None):
    # Seed is used to to always obtain the same random stream, when seed != None.
    random.seed(seed)

    stream = [] # List that keeps track of requests.

    # Initialize the skill to be a random one.
    curr_skill = random_skill(skills)

    # Stream of requests of given length.
    for i in range(length):
        stream.append(curr_skill)
        # Only change the current skill acording to p.
        if random.random() <= 1 / p:
            curr_skill = random_skill(skills)
    
    return stream

# Algorithm that always outsources workers and keeps the cache empty
def always_outsource(stream, workers):
    cost = 0
    history = [0]

    # For each request in the stream, outsource
    for r in stream:
        # Find worker with the given skill and add them to the cache.
        selected_worker = find_worker(workers, r)
        # Outsource worker.
        cost = cost + selected_worker["outsourcing_cost"]
        history.append(cost)
    
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
            # Find worker with the given skill and add them to the cache.
            new_worker = find_worker(workers, r)
            # Hire worker.
            cost = cost + new_worker["hiring_cost"]
            # Add worker to the cache.
            cache.append(new_worker)
             # If the cache is too large.
            if len(cache) > size:
                # Remove the oldest worker
                cache.pop(0)
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
        # Find worker with the given skill.
        new_worker = find_worker(workers, r)

        if not in_cache(cache, r):
            # Hire worker
            cost = cost + new_worker["hiring_cost"]
            # Add worker to the cache.
            cache.append(new_worker)
            # If the cache is too large.
            if len(cache) > size:
                # Remove the LRU worker
                remove_lru(cache)
            # Increment the number of calls of the new worker
            new_worker["num_requests"] += 1

        else:
            new_worker["num_requests"] += 1

        history.append(cost)

    return history

# Algorithm that uses the popularity policy.
def popularity(size, stream, workers):
    cost = 0
    cache = []
    history = [0]

    # Add a call tracker to each worker.
    workers = reset_popularity(workers)
    
    # For each request in the stream, apply the popularity policy.
    for r in stream:
        # Find worker with the given skill.
        new_worker = find_worker(workers, r)

        if not in_cache(cache, r):
            # If there is enough space in the cache, hire.
            if len(cache) < size:
                cost = cost + new_worker["hiring_cost"]
                new_worker["num_requests"] += 1
                cache.append(new_worker)
            # If the worker with the given skill occurs more often than the other ones in the cache, hire.
            elif more_popular(cache, new_worker):
                cost = cost + new_worker["hiring_cost"]
                new_worker["num_requests"] += 1
                cache.append(new_worker)
            # Outsource the worker if the ones in the cache are more popular
            else:
                cost = cost + new_worker["outsourcing_cost"]
                new_worker["num_requests"] += 1
        else:
            # Increment the call counter
            new_worker["num_requests"] += 1

        history.append(cost)

    return history

# Algorithm that hires once total outsourcing cost becomes too large
def out_greater_hire(size, stream, workers):
    cost = 0
    cache = []
    history = [0]

    # Reset the counter of each worker.
    workers = reset_total_outsourcing_cost(workers)
    
    # For each request in the stream, apply the outsource greater than hiring policy.
    for r in stream:
        if not in_cache(cache, r):
            # Find worker with the given skill.
            new_worker = find_worker(workers, r)

            # If the worker's total outsourcing costs become more expensive than hiring, hire.
            if new_worker["total_outsourcing_cost"] > new_worker["hiring_cost"]:
                new_worker["total_outsourcing_cost"] = 0.
                cost = cost + new_worker["hiring_cost"]
                cache.append(new_worker)
                # If the cache is too large.
                if len(cache) > size:
                    # Remove the oldest worker
                    cache.pop(0)
            else:
                # Outsource the worker if outsourcing is still cheaper
                cost = cost + new_worker["outsourcing_cost"]
                new_worker["total_outsourcing_cost"] += new_worker["outsourcing_cost"]

        history.append(cost)

    return history

# Algorithm that will hire and outsource based on a primal and a dual.
def primal_dual(size, stream, workers):
    cost = 0
    cache = []
    history = [0]

    # Reset the counter of each worker, which will now be the hiring variable.
    workers = reset_h_variable(workers)

    # For each request in the stream, apply the primal-dual algorithm.
    for r in stream:
        # If the worker requested is not hired.
        if not in_cache(cache, r):
            new_worker = find_worker(workers, r)

            # Check if their hiring variable is greater than 1.
            if new_worker["h"] >= 1.:
                # Hire
                cost = cost + new_worker["hiring_cost"]
                # Restart hiring variable for future iterations
                new_worker["h"] = 0.
                # Add worker to the cache.
                cache.append(new_worker)
                # If the cache is too large.
                if len(cache) > size:
                    # Remove the oldest worker
                    cache.pop(0)
                # PS: In a scenario with a cache of size 1, there is no need for the distinction as to who should be evicted
                # since there is only one option. However, this should be implemented in the case of a cache greater than 1.
            else:
                # Outsource    
                cost = cost + new_worker["outsourcing_cost"]
                # Update hiring variable
                new_worker["h"] = new_worker["h"] * (1. + (1. / new_worker["hiring_cost"])) + (
                        1. / (size * new_worker["hiring_cost"]))
        
        history.append(cost)
    
    return history
            
# Helper function that reads the skills tsv file and stores it into an output list.     
def load_skills():
    skills_tsv = open(skills_file)
    skills = list(csv.reader(skills_tsv, delimiter="\t"))
    skills_tsv.close()
    skills = list(int(record[0]) for record in skills)
    return skills

# Helper function that reads the workers tsv file and stores it into an output list. 
def load_workers():
    workers_tsv = open(workers_file)
    workers = list(csv.reader(workers_tsv, delimiter="\t"))
    workers_tsv.close()
    list_ws = []
    for w in workers:
        new_w = {}
        new_w["id"] = int(w[0])
        new_w["hiring_cost"] = float(w[3])
        new_w["outsourcing_cost"] = float(w[4])
        new_w["skill_id"] = int(w[-1])

        list_ws.append(new_w)

    return list_ws

# Helper function that returns a random skill from all the possible ones.
def random_skill(skills):
    return random.choice(skills)

# Helper function that finds the worker with the given skill id and returns them.
def find_worker(workers, skill_id):
    for worker in workers:
        if worker["skill_id"] == skill_id:
            return worker
    return False

# Helper function that add as counter to each worker
def add_counter(workers):
    for worker in workers:
        worker["num_requests"] = 0

    return workers

# Helper function that resets the counter of each worker
def reset_popularity(workers):
    for worker in workers:
        worker["popularity"] = 0.

    return workers

# Helper function that resets the counter of each worker
def reset_total_outsourcing_cost(workers):
    for worker in workers:
        worker["total_outsourcing_cost"] = 0.

    return workers

# Helper function that resets the counter of each worker
def reset_h_variable(workers):
    for worker in workers:
        worker["h"] = 0.

    return workers

# Helper function that returns true if the worker is in the cache and false otherwise. 
def in_cache(cache, skill_id):
    for worker in cache:
        if worker["skill_id"] == skill_id:
            return True
    return False

# Helper function that returns true if the given worker is more poular than some other worker in the cache.
# It removes the worker with the lowest popularity if true.
def more_popular(cache, new_worker):
    min_popularity = 1000
    count = 0
    delete = -1
    for worker in cache:
        if worker["popularity"] < min_popularity:
            min_popularity = worker["popularity"]
            delete = count
        count += 1

    if min_popularity < new_worker["popularity"]:
        cache.pop(delete)
        return True
    
    return False

def remove_lru(cache):
    minimum = 1000
    count = 0
    delete = 0

    for worker in cache:
        if worker["num_requests"] < minimum:
            min = worker["num_requests"]
            delete = count

    cache.pop(delete)


if __name__ == "__main__":
    main()