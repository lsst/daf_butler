

class StorageClassMeta(type):

    def __init__(self, name, bases, dct):
        super().__init__(name, bases, dct)
        if hasattr(self, "name"):
            StorageClass.subclasses[self.name] = self


class StorageClass(metaclass=StorageClassMeta):

    subclasses = dict()

    components = dict()

    @classmethod
    def assemble(cls, parent, components):
        return parent


class TablePersistable(StorageClass):
    name = "TablePersistable"


class Image(StorageClass):
    name = "Image"


class Exposure(StorageClass):
    name = "Exposure"
    components = {
        "image": Image,
        "mask": Image,
        "variance": Image,
        "wcs": TablePersistable,
        "psf": TablePersistable,
        "photoCalib": TablePersistable,
        "visitInfo": TablePersistable,
        "apCorr": TablePersistable,
        "coaddInputs": TablePersistable,
    }

    @classmethod
    def assemble(cls, parent, components):
        raise NotImplementedError("TODO")


class Catalog(StorageClass):
    name = "Catalog"
