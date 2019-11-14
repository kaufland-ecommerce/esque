from esque.helpers import SingletonMeta


def test_singleton_meta():
    class A(metaclass=SingletonMeta):
        pass

    class B(metaclass=SingletonMeta):
        pass

    assert isinstance(A.get_instance(), A)
    assert A.get_instance() is A.get_instance()
    assert isinstance(B.get_instance(), B)
    assert B.get_instance() is B.get_instance()

    new_a = A()
    A.set_instance(new_a)
    assert A.get_instance() is new_a

    new_b = B()
    B.set_instance(new_b)
    assert B.get_instance() is new_b
