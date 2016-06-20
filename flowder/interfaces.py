from zope.interface import Interface


class ISignalManager(Interface):
    def connect(*a, **kw):
        """
        :param a:
        :param kw:
        :return:
        """

    def disconnect(*a, **kw):
        """
        """