import itertools

class RoundRobin:
    def __init__(self, nodes: list):
        """
        Класс для распределения задач между нодами по алгоритму Round Robin.

        :param nodes: Список нод.
        """
        self.nodes = nodes
        self.current_node_index = 0
        self.rejected_tasks = 0  # Счетчик отклоненных задач

    def distribute_task(self, task_compute_time: float, task_data_size: float):
        """
        Распределяет задачу между нодами по алгоритму Round Robin.

        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        """
        n = len(self.nodes)
        start_index = self.current_node_index

        while True:
            node = self.nodes[self.current_node_index]
            if node.can_accept_task(task_compute_time, task_data_size):
                node.add_task(task_compute_time, task_data_size)
                self.current_node_index = (self.current_node_index + 1) % n
                break

            self.current_node_index = (self.current_node_index + 1) % n
            if self.current_node_index == start_index:

                self.rejected_tasks += 1
                break
        #print(self.current_node_index)

class WeightedRoundRobin:
    def __init__(self, nodes: list):
        """
        Класс для распределения задач между нодами по алгоритму Round Robin.

        :param nodes: Список нод.
        """
        self.nodes = nodes
        self.current_node_index = 0
        self.rejected_tasks = 0  # Счетчик отклоненных задач
        self.nodes_weights = [0.0] * len(nodes)

    def calc_node_weights(self):
        """Считаем вес как количество свободных ресурсов в процегнтах
        чем больше свободных, тем приоритетнее сервер"""
        for i in range(len(self.nodes)):
            self.nodes_weights[i] = abs(self.nodes[i].current_load * 100 - 100) #+ self.nodes[i].bu_power # очень хорошая равномерность


        #print(self.nodes_weights)
        # self.nodes_weights = [1 for _ in range(4)] + [1.22 for _ in range(4)] + [2.2 for _ in range(1)]
        # print(self.nodes_weights)
       #
       # print([round(w, 8) for w in self.nodes_weights], [abs(n.current_load * 100 - 100) for n in self.nodes])

    def distribute_task(self, task_compute_time: float, task_data_size: float):
        """
        Распределяет задачу между нодами по алгоритму Round Robin.

        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        """

        self.calc_node_weights()

        for i in range(len(self.nodes)):
            "Если можем принять задачу, то пропускаем, иначе ставим вес 0"
            if self.nodes[i].can_accept_task(task_compute_time, task_data_size):
                continue
            else:
                self.nodes_weights[i] = 0.0
                #print(self.nodes_weights)

        while True:
            if sum(self.nodes_weights) == 0:
                #print(self.nodes_weights)
                self.rejected_tasks += 1
                break

            # определяем максимальный доступный вес оставшихся нод
            max_available_weights = max(self.nodes_weights)

            # определяем индекс ноды с максимальным весом
            node_index = self.nodes_weights.index(max_available_weights)

            # отдаем задачу
            self.nodes[node_index].add_task(task_compute_time, task_data_size)
           # print(self.nodes_weights, task_compute_time)
            break


import itertools
import math
from typing import Dict, List


class WeightedRoundRobinStatic:
    """Веса задаются серверам  изначально и не меняются в ходе работы
    распределяет задачи в пропорции мощности групп серверов, а внутри группы отдает задачи циклично"""
    def __init__(self, servers: List['Server']):
        self.servers = servers
        self.nodes = servers
        self.current_node_index = 0
        self.rejected_tasks = 0

        # Группировка серверов по мощности
        self.server_groups = self._group_servers_by_power()
        self.group_weights = self._calculate_group_weights()
        self.total_weight = sum(self.group_weights.values())

        # Подготовка детерминированного распределения
        self._setup_weighted_distribution()

        # Циклические генераторы для каждой группы
        self.server_cycles = {
            power: itertools.cycle(group)
            for power, group in self.server_groups.items()
        }

        # Распределение нагрузки в процентах (для информации)
        self.group_distribution = {
            power: (weight / self.total_weight) * 100
            for power, weight in self.group_weights.items()
        }

    def _group_servers_by_power(self) -> Dict[float, List['Server']]:
        """Группирует серверы по их мощности (bu_power)."""
        groups = {}
        for server in self.servers:
            power = server.bu_power
            if power not in groups:
                groups[power] = []
            groups[power].append(server)
        return groups

    def _calculate_group_weights(self) -> Dict[float, float]:
        """Вычисляет суммарный вес каждой группы серверов."""
        return {
            power: sum(server.bu_power for server in servers)
            for power, servers in self.server_groups.items()
        }

    def _setup_weighted_distribution(self):
        """Настраивает детерминированное распределение для дробных весов."""
        # Находим наименьший ненулевой вес
        non_zero_weights = [w for w in self.group_weights.values() if w > 0]
        if not non_zero_weights:
            raise ValueError("All server groups have zero weight")

        min_weight = min(non_zero_weights)

        # Масштабируем веса и округляем вверх
        self._scaled_weights = []
        self._total_scaled_weight = 0

        for power, weight in sorted(self.group_weights.items()):
            if weight > 0:
                scaled = math.ceil(weight / min_weight)
                self._scaled_weights.append((power, scaled))
                self._total_scaled_weight += scaled

        self._group_selection_counter = 0

    def _select_group_by_weight(self) -> float:
        """Выбирает группу серверов пропорционально их весу (дробные веса поддерживаются)."""
        if not hasattr(self, '_scaled_weights'):
            self._setup_weighted_distribution()

        pos = self._group_selection_counter % self._total_scaled_weight
        self._group_selection_counter += 1

        cumulative = 0
        for power, scaled_weight in self._scaled_weights:
            cumulative += scaled_weight
            if pos < cumulative:
                return power

        return self._scaled_weights[-1][0]  # fallback

    def get_next_server(self) -> 'Server':
        """Возвращает следующий сервер для обработки задачи с учётом WRR."""
        selected_power = self._select_group_by_weight()
        return next(self.server_cycles[selected_power])

    def distribute_task(self, task_compute_time: float, task_data_size: float):
        """Распределяет задачу на сервер с учётом WRR."""
        if len(self.server_groups) == 1:
            # Все серверы одинаковые - простой Round Robin
            n = len(self.nodes)
            start_index = self.current_node_index

            while True:
                node = self.nodes[self.current_node_index]
                if node.can_accept_task(task_compute_time, task_data_size):
                    node.add_task(task_compute_time, task_data_size)
                    self.current_node_index = (self.current_node_index + 1) % n
                    break

                self.current_node_index = (self.current_node_index + 1) % n
                if self.current_node_index == start_index:
                    self.rejected_tasks += 1
                    break
        else:
            server = self.get_next_server()
            if server.can_accept_task(task_compute_time, task_data_size):
                server.add_task(task_compute_time, task_data_size)
            else:
                self.rejected_tasks += 1

    def get_distribution_stats(self) -> Dict[float, float]:
        """Возвращает распределение нагрузки между группами в процентах."""
        return self.group_distribution






class LeastConnection:
    def __init__(self, nodes: list):
        """Класс для распределения задач между нодами по алгоритму Least Connection.
            :param nodes: Список нод.
        """
        self.nodes = nodes
        self.current_node_index = 0
        self.nodes_connections = [0] * len(nodes)
        self.rejected_tasks = 0  # Счетчик отклоненных задач

    def updated_nodes_connections(self, nodes):
        for i in range(len(nodes)):
            self.nodes_connections[i] = nodes[i].get_current_tasks_on_node()  # записываем сколько задач на каждой из нод

    def distribute_task(self, task_compute_demand: float, task_data_size: float):
        """ Распределяет задачу между нодами по алгоритму Least Connections.
        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        :param task_id: Идентификатор задачи. """
          # обновляем количество подключений перед распределением задач
        self.updated_nodes_connections(self.nodes)
        #print(self.nodes_connections, self.rejected_tasks)
        for i in range(len(self.nodes)):
            '''Проверяем доступна ли нода и может ли принять задачу,
            после этого смотрим на количество подключений (задач  на ноде) и
            выбираем с минимальным значением'''
            if self.nodes[i].can_accept_task(task_compute_demand, task_data_size):
                continue
            else:
                self.nodes_connections[i] = 5000  # если нода недоступна, то ставим большое число подключений потому что потому
        #print(self.rejected_tasks)
        while True:
            if all(conn == 5000 for conn in self.nodes_connections):  # если все ноды заняты или не могут взять задачу
               # print(self.nodes_connections)
                self.rejected_tasks += 1
                break
            #print(self.nodes_connections)

            min_connections = min(self.nodes_connections)   # определяем минимальное кол-во подключений среди доступных нод
            min_connections_node_index = self.nodes_connections.index(min_connections)  # определяем первый индекс среди доступных нод

            # отдаем задачу
            self.nodes[min_connections_node_index].add_task(task_compute_demand, task_data_size)

            # обновляем количество подключений
            self.updated_nodes_connections(self.nodes)

            break


class WeightedLeastConnection:
    def __init__(self, nodes: list):
        """Класс для распределения задач между нодами по алгоритму Weighted Least Connection.
            :param nodes: Список нод.
        """
        self.nodes = nodes
        self.current_node_index = 0
        self.nodes_connections = [0] * len(nodes)
        self.rejected_tasks = 0  # Счетчик отклоненных задач

        self.nodes_weights = [0.0] * len(nodes)

        self.wlc_weight = [node.bu_power for node in self.nodes]

    def calc_node_weights(self):
        """Считаем вес как количество свободных ресурсов в процегнтах
        чем больше свободных, тем приоритетнее сервер"""
        for i in range(len(self.nodes)):
            self.nodes_weights[i] = abs(self.nodes[i].current_load * 100 - 100)
            #self.nodes_weights[i] = abs(self.nodes[i].current_load * 100 - 100) * self.nodes[i].bu_power ** 2
            #self.nodes_weights[i] = self.nodes[i].bu_power ** (2 * self.nodes[i].bu_power)

    def updated_nodes_connections(self, nodes):
        for i in range(len(nodes)):
            self.nodes_connections[i] = nodes[i].get_current_tasks_on_node()  # записываем сколько задач на каждой из нод

    def calc_wlc_node_weights(self, nodes):
        '''Вычисляем вес нод для Weighted Least Connections
        чем меньше значение, тем лучше
        w = active_connections/normalize_node_weight'''

        # обновляем кол-во подключений, чтобы далее вызывать только функцию calc_wlc_node_weights
        self.calc_node_weights()
        self.updated_nodes_connections(nodes)

        for i in range(len(self.nodes)):
            # вычисляем вес по формуле  w = node_weight/connections +1
            #self.wlc_weight[i] = (self.nodes_connections[i] )/self.nodes_weights[i]
            #self.wlc_weight[i] = self.nodes_weights[i] / (self.nodes_connections[i] + 1)
            #w = [1] * 4 + [1.22] * 7 + [2.2] * 1

            self.wlc_weight[i] = self.nodes_weights[i] / (self.nodes_connections[i] + 1)

        #print([round(w, 4) for w in self.wlc_weight], self.wlc_weight.index(min([w for w in self.wlc_weight])))

    def distribute_task(self, task_compute_demand: float, task_data_size: float):
        """Распределяет задачу между нодами по алгоритму Weighted Least Connections.
        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        """

        # обновляем вес нод
        self.calc_wlc_node_weights(self.nodes)

        for i in range(len(self.nodes)):
            '''Проверяем доступна ли нода и может ли принять задачу,
            после этого смотрим на количество подключений (задач  на ноде) и
            выбираем с минимальным значением'''
            if self.nodes[i].can_accept_task(task_compute_demand, task_data_size):
                continue
            else:
                self.wlc_weight[i] = 0 # если нода недоступна, то ставим ей большой вес

        while True:
            if all(conn == 0 for conn in self.wlc_weight):  # если все ноды заняты или не могут взять задачу
                self.rejected_tasks += 1
                break

            min_available_weights = max(self.wlc_weight)   # определяем минимальный вес среди доступных нод
            min_weight_node_index = self.wlc_weight.index(min_available_weights)  # определяем первый индекс среди доступных нод



            # отдаем задачу
            self.nodes[min_weight_node_index].add_task(task_compute_demand, task_data_size)

            # обновляем вес нод
            #self.calc_wlc_node_weights(self.nodes)
            break