import uuid

from pygear.twisted.resource import WsResource
from pygear.twisted.signal import get_signal_manager

from flowder import signals


class Schedule(WsResource):
    """
    Schedule a spider run task
    """

    def render_POST(self, txrequest):
        if not self.root.request_permitted(txrequest):
            return {
                "node_name": self.root.nodename,
                "status": "error",
                "message": "Request not permitted, your ip address will be logged!"
            }

        task_info = dict((k, v[0]) for k, v in txrequest.args.items())
        try:
            if 'callback_uri' not in task_info:
                raise ValueError('Given message has no callback_uri value.')

            if 'fetch_uri' not in task_info:
                raise ValueError('Given message has no fetch_uri value.')

        except ValueError as e:
            return {"status": "error", "message": e.message}
        task_info['fetch_uri'] = task_info['fetch_uri'].replace(' ', '+')
        task_info['client_uri'] = txrequest.getClientIP()
        jobid = uuid.uuid1().hex
        task_info['job_id'] = jobid
        self.root.scheduler.schedule(task_info)
        signal_manager = get_signal_manager(self.root.app)
        signal_manager.send_catch_log(signal=signals.request_received, jobid=jobid)
        return {"node_name": self.root.nodename, "status": "ok", "job_id": jobid}


class Cancel(WsResource):
    def render_POST(self, txrequest):
        pass

        # args = dict((k, v[0]) for k, v in txrequest.args.items())
        # jobid = args['job_id']
        # signal = args.get('signal', 'TERM')
        # prevstate = None
        # c = self.root.scheduler.cancel(jobid)
        # if c:
        # prevstate = "pending"
        # spiders = self.root.launcher.processes.values()
        # for s in spiders:
        # if s.job == jobid:
        #         s.transport.signalProcess(signal)
        #         prevstate = "running"
        #
        # signal_manager = self.root.app.getComponent(ISignalManager)
        # signal_manager.send_catch_log(signal=signals.request_received, jobid=jobid)
        # return {"node_name": self.root.nodename, "status": "ok", "prevstate": prevstate}


class ListTasks(WsResource):
    def render_POST(self, txrequest):
        pass


class DeleteTask(WsResource):
    def render_POST(self, txrequest):
        pass


class ListJobs(WsResource):
    def render_POST(self, txrequest):
        pass