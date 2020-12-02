import random
import csv
import matplotlib.pyplot as plt
import pprint as pp

skills_file = '/Users/bernardoetrevisan/Desktop/research-f20/skills.tsv'
workers_file = '/Users/bernardoetrevisan/Desktop/research-f20/workers.tsv'

def main():
    skills = load_skills() # Load skills into a list.
    workers = load_workers() # Load workers into a list.

    stream = random_stream(skills, length=1000, p=50, seed=10) # Generates the random stream of requests.

    cost_outsource = always_outsource(stream, workers)
    cost_hire = always_hire(stream, workers)
    cost_lru = lru(stream, workers)
    cost_out_greater_hire = out_greater_hire(stream, workers)
    cost_primal_dual = primal_dual(stream, workers)

    # print("The cost of always outsourcing was $" + str(format(cost_outsource[1000], '.2f')))
    # print("The cost of always hiring was $" + str(format(cost_hire[1000], '.2f')))
    # print("The cost of using the LRU policy was $" + str(format(cost_lru[1000], '.2f')))
    # print("The cost of hiring once outsourcing becomes too expensive was $" + str(format(cost_out_greater_hire[1000], '.2f')))
    # print("The cost of the primal dual algorithm was $" + str(format(cost_primal_dual[1000], '.2f')))

    # Display plots
    plt.plot(cost_outsource, label='Outsource')
    plt.plot(cost_hire, label='Hire')
    plt.plot(cost_lru, label='LRU')
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
def always_hire(stream, workers):
    cost = 0
    cache = {}
    history = [0]

    # For each request in the stream, hire.
    for r in stream:
        # Hire only if the worker is not yet in the cache.
        if r != cache.get("skill_id", -1):
            # Find worker with the given skill and add them to the cache.
            cache = find_worker(workers, r)
            # Hire worker and keep them in the cache.
            cost = cost + cache["hiring_cost"]
        history.append(cost)
    
    return history

# Algorithm that uses the LRU policy.
def lru(stream, workers):
    cost = 0
    cache = {}
    history = [0]

    # Add a call tracker to each worker.
    workers = add_counter(workers)
    
    # For each request in the stream, apply the LRU policy.
    for r in stream:
        if r != cache.get("skill_id", -1):
            # Find worker with the given skill.
            new_worker = find_worker(workers, r)

            # If the worker with the given skill occurs more often than the one in the cache, hire.
            if cache.get("num_requests", -1) <= new_worker.get("num_requests", -1):
                cache = new_worker
                cost = cost + cache["hiring_cost"]
                # Increment the call counter
                cache["num_requests"] += 1
            else:
                # Outsource the worker if the one in the cache is more popular
                cost = cost + new_worker["outsourcing_cost"]
                new_worker["num_requests"] += 1
        else:
            # Increment the call counter
            cache["num_requests"] += 1

        history.append(cost)

    return history

# Algorithm that hires once total outsourcing cost becomes too large
def out_greater_hire(stream, workers):
    cost = 0
    cache = {}
    history = [0]

    # Reset the counter of each worker.
    workers = reset_total_outsourcing_cost(workers)
    
    # For each request in the stream, apply the outsource greater than hiring policy.
    for r in stream:
        if r != cache.get("skill_id", -1):
            # Find worker with the given skill.
            new_worker = find_worker(workers, r)

            # If the worker's total outsourcing costs become more expensive than hiring, hire.
            if new_worker["total_outsourcing_cost"] > new_worker["hiring_cost"]:
                new_worker["total_outsourcing_cost"] = 0.
                cache = new_worker
                cost = cost + cache["hiring_cost"]
            else:
                # Outsource the worker if outsourcing is still cheaper
                cost = cost + new_worker["outsourcing_cost"]
                new_worker["total_outsourcing_cost"] += new_worker["outsourcing_cost"]

        history.append(cost)

    return history

# Algorithm that will hire and outsource based on a primal and a dual.
def primal_dual(stream, workers):
    cost = 0
    c = 1 # Constant C, which is the size of the cache.
    cache = {} # Cache of size 1
    history = [0]


    # Reset the counter of each worker, which will now be the hiring variable.
    workers = reset_h_variable(workers)

    # For each request in the stream, apply the primal-dual algorithm.
    for r in stream:
        # If the worker requested is not hired.
        if r != cache.get("skill_id", -1):
            new_worker = find_worker(workers, r)

            # Check if their hiring variable is greater than 1.
            if new_worker["h"] >= 1.:
                # Restart hiring variable for future iterations
                new_worker["h"] = 0.
                # Add worker to the cache
                cache = new_worker
                # Hire
                cost = cost + cache["hiring_cost"]
                # PS: In a scenario with a cache of size 1, there is no need for the distinction as to who should be evicted
                # since there is only one option. However, this should be implemented in the case of a cache greater than 1.
            else:
                # Outsource    
                cost = cost + new_worker["outsourcing_cost"]
                # Update hiring variable
                new_worker["h"] = new_worker["h"] * (1. + (1. / new_worker["hiring_cost"])) + (
                        1. / (c * new_worker["hiring_cost"]))
        
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
def reset_total_outsourcing_cost(workers):
    for worker in workers:
        worker["total_outsourcing_cost"] = 0.

    return workers

# Helper function that resets the counter of each worker
def reset_h_variable(workers):
    for worker in workers:
        worker["h"] = 0.

    return workers

if __name__ == "__main__":
    main()