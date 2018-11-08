import multiprocessing as mp


class MultiProcess(object):
    def __init__(self, proc_num=None):
        self.processor_num = proc_num if proc_num else mp.cpu_count()

    def calc_range(self, tot_num):
        sub_num = tot_num // self.processor_num + 1
        sub_from = [sub_num * k for k in range(min(tot_num, self.processor_num))]
        sub_to = sub_from[1:] + [tot_num]
        return zip(sub_from, sub_to)

    def part_data(self, range_from, range_to, dataset=None):
        res = None
        if isinstance(dataset, list):
            res = dataset[range_from: range_to]
        elif isinstance(dataset, dict):
            res = list(dataset.items())[range_from: range_to]
            res = {k: v for k, v in res}
        return res

    def main_proc(self, task_fun, task_param, tot_num, tot_data=None):
        pool = mp.Pool(processes=self.processor_num)
        res_list = list()
        range_list = self.calc_range(tot_num)
        share_dict = mp.Manager().dict(task_param)
        for cursor_from, cursor_to in range_list:
            sub_data = self.part_data(cursor_from, cursor_to, tot_data)
            sole_dict = {'cursor_from': cursor_from, 'cursor_to': cursor_to, 'task_data': sub_data}
            sub_res = pool.apply_async(task_fun, args=(sole_dict, share_dict))
            res_list.append(sub_res)
        pool.close()
        pool.join()
        return res_list