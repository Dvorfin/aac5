from node import Server
from distributor import WeightedRoundRobin2
import itertools

servers = [Server(server_id=1, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=2, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=3, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=4, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=5, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=6, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=7, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=8, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=9, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=10, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=11, bu_power=1, bandwidth_bytes=10000),
        Server(server_id=12, bu_power=1, bandwidth_bytes=10000),]



# Пример использования
if __name__ == "__main__":

    distributor = WeightedRoundRobin2(servers)
    print(len(distributor.server_groups))

    count_12 = 0
    count_1_4 = 0
    count_5_11 = 0

    for i in range(1000):
        server = distributor.get_next_server()
        if server.server_id == 12:
            count_12 += 1
        elif (1 <= server.server_id <= 4):
            count_1_4 += 1
        else:
            count_5_11 += 1

        print(f"Задача {i + 1} назначена на {server.server_id}")


    print(count_1_4 / 1000, count_5_11 / 1000, count_12 / 1000)


