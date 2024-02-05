import sys


def error_message_detail(error, error_detail=sys):
    """
    Fetches the error message and frames the error message in redable manner and provided color to the error.

    Args:
        error (str): Short Message/Heading of the error.
        error_detail (str, optional): Complete description of the error. Defaults to sys. Defaults to sys.

    Returns:
        _type_: _description_
    """    
    type, _, exc_tb = error_detail.exc_info()
    file_name = exc_tb.tb_frame.f_code.co_filename
    error_message = f"\n Error in - {file_name}, \n line - {exc_tb.tb_lineno}, \n error - {type.__name__}: \n {str(error)}"

    return "\33[31m" + error_message + "\33[0m"


class CustomException(Exception):
    """
    Class for raising the Custom Exception and parses the different information of the error.

    Args:
        Exception (object): Error object which has been raised.
    """    
    def __init__(self, error_message, error_detail=sys):
        """
        Initializes an CustomException object.

        Args:
            error_message (str): Short Message/Heading of the error.
            error_detail (str, optional): Complete description of the error. Defaults to sys.
        """        
        super().__init__(error_message)
        self.error_message = error_message_detail(error_message, error_detail=error_detail)

    def __str__(self):
        """
        Defaults to the string of the error object.

        Returns:
            str: Returns the complete description of the error.
        """        
        return self.error_message
