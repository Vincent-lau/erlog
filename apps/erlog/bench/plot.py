import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit


def get_graph_size(file_name):
    num_nodes = 0
    num_edges = 0
    with open(file_name, 'r') as in_file:
        for line in in_file:
            if line[0:4] != 'link':
                continue
            num_edges += 1
            l = line.strip("link.\n()")
            n1, n2 = l.split(",")
            n1 = n1.strip('a"')
            n2 = n2.strip('a" ')
            num_nodes = max(num_nodes, max(int(n1), int(n2)))
            print(n1, n2)
    return (num_nodes, num_edges)


def read_data(file_name):
    times = []
    workers = []
    throughput = []
    with open(file_name, "r") as in_file:
        for l in in_file:
            line = l.strip("{},\n")
            time, num_workers = line.split(",")
            time = float(time) / 1000
            throughput.append(float(num_edges) / float(time))
            times.append(round(float(time), 2))
            workers.append(num_workers)
    return (throughput, workers)


def read_data2(file_name, num_edges):
    workers = []
    throughput = []
    times = []
    with open(file_name, "r") as in_file:
        for l in in_file:
            if l[0] == '=':
                break
            elif l[0] == 'r':
                continue
            line = l.strip(" {}[],\n")
            line = line.replace("]", "")
            line = line.replace(" ", "")
            data = line.split(",")
            workers.append(int(data[-1]))
            times.append(data[0: -1])
    for t in times:
        throughput.append([num_edges / (float(x) / 1000 / 1000) for x in t])
    return (throughput, workers)


def func(x, a, b, c, d):
    print(x)
    return a * (x**3) + b*(x**2) + c * x + d


def plot(throughput, workers):
    # confidence interval
    max_throughputs = [np.max(t) for t in throughput]
    min_throughputs = [np.min(t) for t in throughput]
    mean_throughputs = [np.mean(t) for t in throughput]

    # create flattened version
    workers_flat = np.array([[w] * 10 for w in workers]).flatten()
    throughput_flat = throughput.flatten()

    # fit the curve
    popt, pcov = curve_fit(func, workers, mean_throughputs)

    fig, ax = plt.subplots()
    ax.set_title("throughput against #workers")
    ax.set_xlabel("number of workers")
    ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax.set_ylabel(
        "size of graph (#edges) divided by time measured by timer:tc in second")

    ax.scatter(workers_flat, throughput_flat, s=6, alpha=0.8)
    ax.fill_between(workers, min_throughputs, max_throughputs,
                    color='yellow', alpha=0.3)
    ax.plot(workers, func(workers, *popt), color='orange')

    fig.show()
    plt.show()


(throughput, workers) = read_data2("time_worker.txt", 2000)
throughput = np.array(throughput)
workers = np.array(workers)

plot(throughput, workers)
