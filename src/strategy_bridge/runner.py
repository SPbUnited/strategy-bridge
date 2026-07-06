import logging
from multiprocessing import Process
from multiprocessing.connection import wait

from multiprocessing.managers import BaseManager

import typing

import attr

from strategy_bridge.bus import DataBus
from strategy_bridge.processors import BaseProcessor


class BridgeManager(BaseManager):
    pass


@attr.s(auto_attribs=True, kw_only=True)
class Runner:
    processors: typing.List[BaseProcessor]
    logger: logging.Logger = logging.getLogger(__name__)

    def run(self):
        BridgeManager.register('data_bus', DataBus)
        with BridgeManager() as manager:
            data_bus = manager.data_bus()
            processes = [
                Process(target=self.run_processor, args=(processor, data_bus)) for processor in self.processors
            ]
            for process in processes:
                process.start()
            try:
                self.wait_for_processes(processes)
            except KeyboardInterrupt:
                self.logger.warning("The application was interrupted")
            finally:
                self.stop_processes(processes)

    def wait_for_processes(self, processes: typing.List[Process]) -> None:
        remaining = list(processes)
        while remaining:
            for sentinel in wait([process.sentinel for process in remaining]):
                finished = next(process for process in remaining if process.sentinel == sentinel)
                remaining.remove(finished)
                if finished.exitcode != 0:
                    self.logger.error(
                        f"Process {finished.name} exited with code {finished.exitcode}, stopping the rest"
                    )
                    return

    def stop_processes(self, processes: typing.List[Process]) -> None:
        for process in processes:
            if process.is_alive():
                process.terminate()
        for process in processes:
            process.join()

    def run_processor(self, processor: BaseProcessor, data_bus: DataBus) -> None:
        processor.initialize(data_bus)
        processor.run()
