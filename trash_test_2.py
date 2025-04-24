from node import Server
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
    def generate_repeating_task_list(total_tasks):
        # Базовый паттерн
        pattern = [0.02] * 6 + [0.1] * 3 + [0.28] * 1
        print(pattern)

        # Вычисляем количество полных паттернов и остаток
        full_patterns = total_tasks // len(pattern)
        remainder = total_tasks % len(pattern)

        # Формируем итоговый список
        task_list = pattern * full_patterns + pattern[:remainder]

        return task_list


    # Пример использования
    total_tasks = 300  # Сколько всего задач сгенерировать
    task_list = generate_repeating_task_list(total_tasks)
    print(task_list)
    print(sum(task_list))
    #print(f"Список задач: {' '.join(task_list)}")
    print(
        f"Количество задач каждого типа: S={task_list.count(0.02)}, M={task_list.count(0.1)}, H={task_list.count(0.28)}")


