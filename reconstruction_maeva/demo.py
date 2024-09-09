from progress.bar import Bar
import time

# Example usage
total_items = 100
bar = Bar('Processing', max=total_items)
for i in range(total_items):
    time.sleep(1)  # Simulate work
    bar.next()
    print('\n next item')
bar.finish()
