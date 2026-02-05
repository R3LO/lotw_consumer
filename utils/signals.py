"""
Модуль для обработки сигналов
"""

import signal


def get_signal_name(signum):
    """Возвращает имя сигнала"""
    signal_names = {
        signal.SIGINT: "SIGINT",
        signal.SIGTERM: "SIGTERM"
    }
    return signal_names.get(signum, str(signum))


def setup_signal_handlers(consumer):
    """Настройка обработчиков сигналов"""
    signal.signal(signal.SIGINT, consumer.signal_handler)
    signal.signal(signal.SIGTERM, consumer.signal_handler)