class DelimiterError(Exception):
    error_code = 'Delimiter used in the file is not with the expected format. Please check and try again.'

    def __str__(self):
        return self.error_code


class ColNotFoundError(Exception):
    error_code = 'Mandatory fields are missing in given file. Please check and try again.'

    def __str__(self):
        return self.error_code


class ColExtraFoundError(Exception):
    error_code = 'Extra Columns found in given file. Please check and try again.'

    def __str__(self):
        return self.error_code


class ColLessFoundError(Exception):
    error_code = 'Minimum no of Columns are not present in csv file. Please check and try again.'

    def __str__(self):
        return self.error_code


class DataDuplicateError(Exception):
    error_code = 'Data is duplicated for few months or days. Please check and try again.'

    def __str__(self):
        return self.error_code


class DataDuplicateError1(Exception):
    error_code = 'Data is duplicated in Key Columns. Please check and try again.'

    def __str__(self):
        return self.error_code


class DataDuplicateError2(Exception):
    error_code = 'Data is duplicated. Please check and try again.'

    def __str__(self):
        return self.error_code


class ValueNegativeError(Exception):
    error_code = 'Invalid ALP values. Values must not be negative.'

    def __str__(self):
        return self.error_code


class DataNotFoundError(Exception):
    error_code = 'Data missing for few months or days. Please check and try again.'

    def __str__(self):
        return self.error_code


class RowEmptyError(Exception):
    error_code = 'Data missing in some of the rows. Please check and try again.'

    def __str__(self):
        return self.error_code
