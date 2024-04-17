
# write a function that takes a list of numbers and returns the sum of the squares of the numbers
def sum_of_squares(numbers):
    return sum([n**2 for n in numbers])

# write a function that takes a number and returns factorial of that number
def factorial(n):
    if n == 0:
        return 1
    return n * factorial(n-1)

# write a function that takes a list of numbers and returns the sum of the cubes of the numbers
def sum_of_cubes(numbers):
    return sum([n**3 for n in numbers])


# write a function that takes a number and returns True if it is prime, False otherwise
def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True
