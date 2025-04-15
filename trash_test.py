from typing import List, Dict, Callable
import random


class Server:
    """Класс сервера с индивидуальными параметрами"""

    def __init__(self, server_id: int, task_duration: float, capacity: float = None):
        self.id = server_id
        self.task_duration = task_duration
        self.capacity = capacity if capacity is not None else (
            1.0 / task_duration if task_duration > 0 else float('inf'))
        self.current_load = 0.0
        self.processed_tasks = 0
        self.dropped_tasks = 0
        self.rejected_tasks = 0  # Новый счетчик для явно отклоненных задач
        self.cpu_load_history = []
        self.tasks_history = []
        self.rejected_history = []  # История отклоненных задач по секундам

    def reset_for_new_second(self):
        """Сброс состояния для новой секунды симуляции"""
        self.current_load = 0.0
        self.cpu_load_history.append(0.0)
        self.tasks_history.append(0)
        self.rejected_history.append(0)

    def can_accept_task(self):
        """Проверяет, может ли сервер принять новую задачу"""
        return self.current_load < self.capacity

    def process_task(self):
        """Обрабатывает задачу и возвращает True, если задача принята"""
        if self.can_accept_task():
            self.current_load += 1
            self.processed_tasks += 1
            self.tasks_history[-1] += 1
            return True
        return False

    def reject_task(self):
        """Увеличивает счетчик отклоненных задач"""
        self.rejected_tasks += 1
        self.rejected_history[-1] += 1
        self.dropped_tasks += 1

    def calculate_load(self):
        """Рассчитывает текущую нагрузку сервера"""
        return min(100.0, (self.current_load / self.capacity) * 100) if self.capacity > 0 else 0.0


class BalancerAlgorithms:
    """Класс с реализацией алгоритмов балансировки"""

    @staticmethod
    def round_robin(servers: List[Server], state: Dict) -> int:
        """Алгоритм Round Robin"""
        current = state.get('last_selected', -1)
        for i in range(1, len(servers) + 1):
            next_server = (current + i) % len(servers)
            if servers[next_server].can_accept_task():
                state['last_selected'] = next_server
                return next_server
        return -1  # Все серверы перегружены

    @staticmethod
    def weighted_round_robin(servers: List[Server], state: Dict, params: Dict) -> int:
        """Алгоритм Weighted Round Robin"""
        weights = params.get('weights', [1] * len(servers))
        current = state.get('current', 0)
        counter = state.get('counter', 0)

        for _ in range(sum(weights)):  # Максимальное число попыток
            if counter >= weights[current]:
                current = (current + 1) % len(servers)
                counter = 0
                continue

            if servers[current].can_accept_task():
                state['current'] = current
                state['counter'] = counter + 1
                return current

            current = (current + 1) % len(servers)
            counter = 0

        return -1  # Все серверы перегружены

    @staticmethod
    def least_connections(servers: List[Server], state: Dict) -> int:
        """Алгоритм Least Connections"""
        available = []
        min_load = float('inf')

        for i, server in enumerate(servers):
            if server.can_accept_task():
                load = server.current_load / server.capacity if server.capacity > 0 else 0
                if load < min_load:
                    min_load = load
                    available = [i]
                elif load == min_load:
                    available.append(i)

        return random.choice(available) if available else -1

    @staticmethod
    def random_selection(servers: List[Server], state: Dict) -> int:
        """Случайный выбор сервера"""
        available = [i for i, s in enumerate(servers) if s.can_accept_task()]
        return random.choice(available) if available else -1


class LoadBalancer:
    """Балансировщик нагрузки"""

    def __init__(self):
        self.algorithm = None
        self.algorithm_state = {}
        self.algorithm_params = {}

    def set_algorithm(self, algorithm: Callable, initial_state: Dict = None, algorithm_params: Dict = None):
        """Устанавливает алгоритм балансировки"""
        self.algorithm = algorithm
        self.algorithm_state = initial_state or {}
        self.algorithm_params = algorithm_params or {}

    def select_server(self, servers: List[Server]) -> int:
        """Выбирает сервер для обработки задачи"""
        if not self.algorithm:
            raise ValueError("No balancing algorithm selected")

        if self.algorithm.__name__ == 'weighted_round_robin':
            return self.algorithm(servers, self.algorithm_state, self.algorithm_params)
        return self.algorithm(servers, self.algorithm_state)

    def distribute_task(self, servers: List[Server]) -> bool:
        """Распределяет задачу на сервер и возвращает True, если задача была принята"""
        selected_idx = self.select_server(servers)
        if selected_idx == -1:
            # Все серверы перегружены - отклоняем задачу
            # Записываем отклонение на сервер, который должен был обрабатывать
            if self.algorithm.__name__ == 'round_robin':
                last = self.algorithm_state.get('last_selected', -1)
                if last != -1:
                    servers[last].reject_task()
            elif self.algorithm.__name__ == 'weighted_round_robin':
                current = self.algorithm_state.get('current', 0)
                servers[current].reject_task()
            return False

        if not servers[selected_idx].process_task():
            servers[selected_idx].reject_task()
            return False
        return True


class CPULoadSimulator:
    """Симулятор нагрузки CPU"""

    def __init__(self):
        self.servers = []
        self.balancer = LoadBalancer()
        self.global_rejected = 0  # Глобальный счетчик отклоненных задач

    def add_server(self, task_duration: float, capacity: float = None) -> Server:
        """Добавляет сервер в симуляцию"""
        server = Server(len(self.servers), task_duration, capacity)
        self.servers.append(server)
        return server

    def set_default_algorithm(self):
        """Устанавливает алгоритм по умолчанию (Round Robin)"""
        self.balancer.set_algorithm(
            algorithm=BalancerAlgorithms.round_robin,
            initial_state={'last_selected': -1}
        )

    def simulate(self, tasks_per_second: float, simulation_time: float) -> Dict:
        """Запускает симуляцию"""
        if not self.servers:
            raise ValueError("No servers configured")
        if not self.balancer.algorithm:
            self.set_default_algorithm()

        self.global_rejected = 0

        for _ in range(int(simulation_time)):
            # Сброс нагрузки для новой секунды
            for server in self.servers:
                server.reset_for_new_second()

            # Обработка задач в текущей секунде
            for _ in range(int(tasks_per_second)):
                if not self.balancer.distribute_task(self.servers):
                    self.global_rejected += 1

            # Расчет нагрузки для каждого сервера
            for server in self.servers:
                server.cpu_load_history[-1] = server.calculate_load()

        return {
            'servers': self.servers,
            'total_dropped': sum(s.dropped_tasks for s in self.servers),
            'total_rejected': self.global_rejected,
            'simulation_time': simulation_time,
            'algorithm': self.balancer.algorithm.__name__
        }

    @staticmethod
    def print_results(results: Dict):
        """Выводит результаты симуляции"""
        print(f"\nРезультаты симуляции ({results['simulation_time']} сек)")
        print(f"Алгоритм балансировки: {results['algorithm']}")
        print(f"Всего отклонено задач: {results['total_rejected']}")
        print(f"Всего потеряно задач (включая перегруженные серверы): {results['total_dropped']}")
        print("-" * 60)

        for server in results['servers']:
            print(f"\nСервер {server.id} (время задачи: {server.task_duration:.2f} сек, "
                  f"пропускная способность: {server.capacity:.1f} задач/сек):")
            print(f"{'Секунда':<10}{'Нагрузка':<10}{'Задач выполнено':<15}{'Отклонено':<10}")
            for sec, (load, tasks, rejected) in enumerate(zip(
                    server.cpu_load_history,
                    server.tasks_history,
                    server.rejected_history
            )):
                print(f"{sec:<10}{load:<10.1f}%{tasks:<15}{rejected:<10}")
            print(f"Всего выполнено: {server.processed_tasks}")
            print(f"Всего отклонено: {server.rejected_tasks}")
            print(f"Всего потеряно: {server.dropped_tasks}")


# Пример использования
if __name__ == "__main__":
    # 1. Создаем симулятор
    simulator = CPULoadSimulator()

    # 2. Добавляем серверы с разной производительностью
    simulator.add_server(task_duration=0.02)  # Сервер 0: ~3.33 задач/сек
    simulator.add_server(task_duration=0.02)  # Сервер 1: 5 задач/сек


    # 3. Настраиваем алгоритм балансировки
    simulator.balancer.set_algorithm(
        algorithm=BalancerAlgorithms.round_robin,
        initial_state={'current': 0, 'counter': 0},
        algorithm_params={'weights': [1, 2, 1]}  # Веса для серверов
    )

    # 4. Запускаем симуляцию с перегрузкой
    results = simulator.simulate(
        tasks_per_second=25,  # Больше, чем могут обработать серверы
        simulation_time=10  # 5 секунд симуляции
    )

    # 5. Выводим результаты
    CPULoadSimulator.print_results(results)


class WeightedRoundRobin:
    def __init__(self, server_weights):
        """
        Инициализация WRR с весами серверов.

        :param server_weights: Список кортежей вида (имя сервера, вес).
        """
        self.server_weights = server_weights
        self.groups = {}  # Группы серверов с одинаковыми весами
        self.total_weight = 0  # Общий вес всех серверов

        # Группируем серверы по весам
        for server, weight in server_weights:
            if weight not in self.groups:
                self.groups[weight] = []
            self.groups[weight].append(server)
            self.total_weight += weight

        # Сортируем веса по убыванию
        self.sorted_weights = sorted(self.groups.keys(), reverse=True)

        # Текущее состояние распределения
        self.current_index = 0  # Индекс текущего веса
        self.group_counters = {weight: 0 for weight in self.sorted_weights}  # Счетчики для каждой группы

    def get_next_server(self):
        """
        Получить следующий сервер для обработки задачи согласно WRR.

        :return: Имя сервера, которому нужно отправить задачу.
        """
        while True:
            # Выбираем текущий вес
            current_weight = self.sorted_weights[self.current_index]
            servers_in_group = self.groups[current_weight]

            # Выбираем сервер из текущей группы
            server_index = self.group_counters[current_weight] % len(servers_in_group)
            server = servers_in_group[server_index]

            # Увеличиваем счетчик для текущей группы
            self.group_counters[current_weight] += 1

            # Переходим к следующему весу
            self.current_index = (self.current_index + 1) % len(self.sorted_weights)

            return server


# Пример использования
if __name__ == "__main__":
    # Определяем веса серверов
    server_weights = [
        ("Server_A", 1),
        ("Server_B", 2),
        ("Server_C", 2),
        ("Server_D", 4),
        ("Server_E", 4),
        ("Server_F", 4)
    ]

    # Создаем экземпляр WRR
    wrr = WeightedRoundRobin(server_weights)

    print(wrr.groups)

    # Распределяем 14 задач
    tasks = 14
    for i in range(tasks):
        server = wrr.get_next_server()
        print(f"Задача {i + 1} назначена на {server}")
