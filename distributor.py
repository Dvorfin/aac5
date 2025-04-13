
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
            self.nodes_weights[i] = abs(self.nodes[i].current_load * 100 - 100) * self.nodes[i].bu_power * 2
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

        for i in range(len(self.nodes)):
            '''Проверяем доступна ли нода и может ли принять задачу,
            после этого смотрим на количество подключений (задач  на ноде) и
            выбираем с минимальным значением'''
            if self.nodes[i].can_accept_task(task_compute_demand, task_data_size):
                continue
            else:
                self.nodes_connections[i] = 5000  # если нода недоступна, то ставим большое число подключений потому что потому

        while True:
            if all(conn == 5000 for conn in self.nodes_connections):  # если все ноды заняты или не могут взять задачу

                self.rejected_tasks += 1
                break

            min_connections = min(self.nodes_connections)   # определяем минимальное кол-во подключений среди доступных нод
            min_connections_node_index = self.nodes_connections.index(min_connections)  # определяем первый индекс среди доступных нод
            #print(self.nodes_connections, min_connections_node_index)
            # отдаем задачу
            self.nodes[min_connections_node_index].add_task(task_compute_demand, task_data_size)

            # обновляем количество подключений
            #self.updated_nodes_connections(self.nodes)

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

        self.wlc_weight = [0.0] * len(nodes)

    def calc_node_weights(self):
        """Считаем вес как количество свободных ресурсов в процегнтах
        чем больше свободных, тем приоритетнее сервер"""
        for i in range(len(self.nodes)):
            self.nodes_weights[i] = abs(self.nodes[i].current_load * 100 - 100) * self.nodes[i].bu_power ** 5
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
            # вычисляем вес по формуле  w = Vc/normalize_node_weight
            self.wlc_weight[i] = self.nodes_connections[i]/self.nodes_weights[i]
        print([round(w, 4) for w in self.wlc_weight], self.wlc_weight.index(min([w for w in self.wlc_weight])))

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
                self.wlc_weight[i] = 5000 # если нода недоступна, то ставим ей большой вес

        while True:
            if all(conn == 5000 for conn in self.wlc_weight):  # если все ноды заняты или не могут взять задачу
                self.rejected_tasks += 1
                break

            min_available_weights = min(self.wlc_weight)   # определяем минимальный вес среди доступных нод
            min_weight_node_index = self.wlc_weight.index(min_available_weights)  # определяем первый индекс среди доступных нод



            # отдаем задачу
            self.nodes[min_weight_node_index].add_task(task_compute_demand, task_data_size)

            # обновляем вес нод
            #self.calc_wlc_node_weights(self.nodes)
            break