import unittest
# import utils

class TestUtils(unittest.TestCase):
    def test_sum_of_squares(self):
        self.assertEqual(utils.sum_of_squares([1, 2, 3]), 14)

    def test_factorial(self):
        self.assertEqual(utils.factorial(5), 120)

    def test_sum_of_cubes(self):
        self.assertEqual(utils.sum_of_cubes([1, 2, 3]), 36)

    def test_is_prime(self):
        self.assertTrue(utils.is_prime(2))
        self.assertTrue(utils.is_prime(3))
        self.assertFalse(utils.is_prime(4))
        self.assertTrue(utils.is_prime(5))

if __name__ == '__main__':
    unittest.main()