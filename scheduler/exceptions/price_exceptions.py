class LdzError(Exception):
    error_code = 'E_LDZ_D'

    def __str__(self):
        return self.error_code


class ExitZoneError(Exception):
    error_code = 'E_EXZ_D'

    def __str__(self):
        return self.error_code


class LdzExzNotMatchError(Exception):
    error_code = 'E_LdzExz_D'

    def __str__(self):
        return self.error_code
