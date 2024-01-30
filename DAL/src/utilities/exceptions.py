import sys


def error_message_detail(error, error_detail=sys):
    type, _, exc_tb = error_detail.exc_info()
    file_name = exc_tb.tb_frame.f_code.co_filename
    error_message = f"\n Error in - {file_name}, \n line - {exc_tb.tb_lineno}, \n error - {type.__name__}: \n {str(error)}"

    return "\33[31m" + error_message + "\33[0m"


class CustomException(Exception):
    def __init__(self, error_message, error_detail=sys):
        super().__init__(error_message)
        self.error_message = error_message_detail(error_message, error_detail=error_detail)

    def __str__(self):
        return self.error_message
