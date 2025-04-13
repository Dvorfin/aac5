
class Server:
    def __init__(self, server_id: int, bu_power: float,
                 bandwidth_bytes: float = 0.0, failure_probability: float = 0.0, downtime_seconds: float =0.0):
        """
        Класс вычислительной ноды.

        :param node_id: Номер ноды.
        :param compute_power_flops: Мощность ноды в FLOPS.
        """
        self.server_id = server_id
        self.bu_power = bu_power
        self.bandwidth_bytes = bandwidth_bytes
        self.current_load = 0.0     # текущая нагрузка в секундах
        self.current_network_load_bytes = 0.0

        self.cpu_load_history = []
        self.network_load_history = []
        self.tasks_history = []

        self.processed_tasks = 0
        self.dropped_tasks = 0
        self.total_work_time = 0

    def calc_tasks_execution_time(self, task_bu):
        return task_bu / self.bu_power

    def can_accept_task(self, task_compute_time: float, task_data_size: float) -> bool:
        if (self.current_load + self.calc_tasks_execution_time(task_compute_time) <= 1 and
        self.current_network_load_bytes + task_data_size <= self.bandwidth_bytes):
            return True
        else:
            self.dropped_tasks += 1
            return False

    def calculate_load(self):
        "Считаем нагрузку сервера в проценгтах за секунду"
        return min(100.0, (self.current_load / self.bu_power) * 100) if self.current_load > 0 else 0.0

    def calculate_network_load(self):
        return min(100.0, (self.current_network_load_bytes / self.bandwidth_bytes) * 100) if self.current_network_load_bytes > 0 else 0.0

    def add_task(self, task_compute_time, task_data_size):
        self.current_load += self.calc_tasks_execution_time(task_compute_time)
        self.total_work_time += task_compute_time / self.bu_power
        self.current_network_load_bytes += task_data_size

        self.processed_tasks += 1
        self.tasks_history[-1] += 1
       # self.cpu_load_history[-1] = self.calculate_load()
        self.cpu_load_history[-1] = self.current_load * 100
        self.network_load_history[-1] = self.calculate_network_load()

    def reset_for_new_second(self):
        self.current_load = 0.0
        self.current_network_load_bytes = 0.0

        self.cpu_load_history.append(0.0)
        self.network_load_history.append(0.0)
        self.tasks_history.append(0)

    def get_current_tasks_on_node(self):
        #print(self.tasks_history[-1])
        return self.tasks_history[-1]





