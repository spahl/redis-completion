"""
Redis-style ZSET in python

Implemented using a skip-list, based on
http://code.activestate.com/recipes/576930/
"""
import random
import sys
import unittest
from collections import deque
from itertools import islice
from math import log, ceil


class Node(object):
    __slots__ = ('key', 'value', 'next', 'width')

    def __init__(self, key, value, next, width):
        self.key = key
        self.value = value
        self.next = next
        self.width = width

    def __repr__(self):
        return '%s: %s (width=%s)' % (self.key, self.value, self.width)


class End(object):
    # sentinel object that always compares greater than another
    def __cmp__(self, other):
        return 1

NIL = Node(End(), End(), [], []) # singleton terminator node


class SkipList(object):
    def __init__(self, expected_size=100, score_unique=False):
        self.size = 0
        self.maxlevels = int(1 + log(expected_size, 2))
        self.head = Node('HEAD', 'HEAD', [NIL] * self.maxlevels, [1] * self.maxlevels)
        self.keys = {}
        self.score_unique = score_unique

    def __len__(self):
        return self.size

    def __contains__(self, k):
        return k in self.keys

    def __or__(self, rhs):
        # union
        new_list = SkipList(self.size + rhs.size, self.score_unique)
        for key, value in self.keys.iteritems():
            new_list.insert(key, value)
        for key, value in rhs.keys.iteritems():
            if key not in new_list:
                new_list.insert(key, value)
        return new_list

    def __and__(self, rhs):
        # intersection
        new_list = SkipList(self.size, self.score_unique)
        for key, value in self.keys.iteritems():
            if key in rhs:
                new_list.insert(key, value)
        return new_list

    def get_slice(self, node, s):
        start = s.start
        stop = s.stop
        if stop < 0:
            stop = self.size + stop
        step = s.step or 1
        for i in range(stop - start):
            if step == 1 or i % step == 0:
                yield (node.key, node.value)
            node = node.next[0]

    def __getitem__(self, idx):
        node = self.head
        if isinstance(idx, slice):
            start = idx.start
            is_slice = True
        elif isinstance(idx, int):
            if idx < 0:
                idx = self.size + idx
            start = idx
            is_slice = False
        else:
            return self.keys[idx]

        start += 1

        for level in reversed(range(self.maxlevels)):
            while node.width[level] <= start:
                start -= node.width[level]
                node = node.next[level]

        if is_slice:
            return list(self.get_slice(node, idx))

        return (node.key, node.value)

    def insert(self, key, value=0):
        if key in self:
            if self[key] == value:
                return
            else:
                self.remove(key)

        # find first node on each level where node.next[levels].value > value
        unique = self.score_unique
        chain = [None] * self.maxlevels
        steps_at_level = [0] * self.maxlevels
        node = self.head
        for level in reversed(range(self.maxlevels)):
            while (node.next[level].value, node.next[level].key) <= (value, key):
                steps_at_level[level] += node.width[level]
                node = node.next[level]
            if unique and node.value == value:
                return False
            chain[level] = node

        # insert a link to the new node at each level
        d = min(self.maxlevels, 1 - int(log(random.random(), 2.0)))
        new_node = Node(key, value, [None] * d, [None] * d)
        steps = 0
        for level in range(d):
            prev_node = chain[level]
            new_node.next[level] = prev_node.next[level]
            prev_node.next[level] = new_node
            new_node.width[level] = prev_node.width[level] - steps
            prev_node.width[level] = steps + 1
            steps += steps_at_level[level]

        for level in range(d, self.maxlevels):
            chain[level].width[level] += 1

        self.keys[key] = value
        self.size += 1

    def remove(self, key):
        value = self.keys[key]
        chain = [None] * self.maxlevels
        node = self.head
        for level in reversed(range(self.maxlevels)):
            while (node.next[level].value, node.next[level].key) < (value, key):
                node = node.next[level]
            chain[level] = node

        if value != chain[0].next[0].value:
            raise KeyError('Not found')

        # remove one link at each level
        d = len(chain[0].next[0].next)
        for level in range(d):
            prev_node = chain[level]
            prev_node.width[level] += prev_node.next[level].width[level] - 1
            prev_node.next[level] = prev_node.next[level].next[level]

        for level in range(d, self.maxlevels):
            chain[level].width[level] -= 1

        del(self.keys[key])
        self.size -= 1

    def __iter__(self):
        node = self.head.next[0]
        while node is not NIL:
            yield (node.key, node.value)
            node = node.next[0]


class SkipListTestCase(unittest.TestCase):
    ubuntu_versions = (
        ('Hardy', 8.04),
        ('Intrepid', 8.1),
        ('Jaunty', 9.04),
        ('Karmic', 9.1),
        ('Lucid', 10.04),
        ('Maverick', 10.1),
        ('Natty', 11.04),
        ('Oneiric', 11.1),
        ('Precise', 12.04),
    )

    def randomize(self, data):
        sl = SkipList(len(data))
        idx = range(len(data))
        random.shuffle(idx)
        for i in idx:
            sl.insert(*data[i])
        return sl

    def test_insert_iterate(self):
        sl = SkipList(10)
        sl.insert('k1', 1)
        self.assertEqual(list(sl), [('k1', 1)])

        sl.insert('k2', 2)
        sl.insert('k99', 0)
        self.assertEqual(list(sl), [
            ('k99', 0), ('k1', 1), ('k2', 2),
        ])

        sl.insert('k0', 2)
        self.assertEqual(list(sl), [
            ('k99', 0), ('k1', 1), ('k0', 2), ('k2', 2),
        ])

    def test_remove(self):
        sl = SkipList(10)
        sl.insert('k1', 1)
        sl.insert('k2', 2)
        sl.insert('k99', 0)
        sl.insert('k0', 2)

        sl.remove('k2')
        self.assertEqual(list(sl), [
            ('k99', 0), ('k1', 1), ('k0', 2),
        ])

        sl.remove('k99')
        self.assertEqual(list(sl), [
            ('k1', 1), ('k0', 2),
        ])

    def test_indexing_ordinal(self):
        sl = self.randomize(self.ubuntu_versions)
        indices = [0, -1, 8, 4]
        for i in indices:
            self.assertEqual(sl[i], self.ubuntu_versions[i])
        self.assertRaises(IndexError, sl.__getitem__, 9)

    def test_indexing_slice(self):
        sl = self.randomize(self.ubuntu_versions)
        slices = [slice(0, 0), slice(0, 2), slice(1, 5, 2), slice(5, 0), slice(0, -1), slice(2, -2)]
        for s in slices:
            self.assertEqual(sl[s], list(self.ubuntu_versions[s]))

    def test_indexing_key(self):
        sl = self.randomize(self.ubuntu_versions)
        self.assertEqual(sl['Hardy'], 8.04)
        self.assertEqual(sl['Precise'], 12.04)
        self.assertRaises(KeyError, sl.__getitem__, 'Foo')

    def _get_lists(self):
        sl1 = SkipList(10)
        sl2 = SkipList(10)
        d1 = [('k1', 1), ('k2', 2), ('k3', 3)]
        d2 = [('k2', 2), ('k3', 33), ('k4', 4), ('k99', 2)]
        for (s, d) in ((sl1, d1), (sl2, d2)):
            map(lambda d: s.insert(*d), d)
        return sl1, sl2

    def test_union(self):
        sl1, sl2 = self._get_lists()
        self.assertEqual(list(sl1 | sl2), [('k1', 1), ('k2', 2), ('k99', 2), ('k3', 3), ('k4', 4)])
        self.assertEqual(list(sl2 | sl1), [('k1', 1), ('k2', 2), ('k99', 2), ('k4', 4), ('k3', 33)])

    def test_intersection(self):
        sl1, sl2 = self._get_lists()
        self.assertEqual(list(sl1 & sl2), [('k2', 2), ('k3', 3)])
        self.assertEqual(list(sl2 & sl1), [('k2', 2), ('k3', 33)])


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(SkipListTestCase)
    results = unittest.TextTestRunner(verbosity=1).run(suite)
    if not results.wasSuccessful():
        sys.exit(1)
