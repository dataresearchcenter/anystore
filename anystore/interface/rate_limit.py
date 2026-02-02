# https://github.com/alephdata/servicelayer/blob/main/servicelayer/rate_limit.py

import time

from anystore.store import Store


class RateLimit:
    """Limit the rate of operations on a given resource during a stated
    interval.

    Uses any anystore backend to track request counts per time slot.

    Example:
        ```python
        from anystore.interface import get_rate_limit

        limit = get_rate_limit("redis://localhost", "my-resource", limit=100, interval=60)
        if limit.check():
            limit.update()
        ```
    """

    def __init__(
        self,
        store: Store,
        resource: str,
        limit: int = 100,
        interval: int = 60,
        unit: int = 1,
    ) -> None:
        self.store = store
        self.store.raise_on_nonexist = False
        self.resource = resource
        self.limit = max(0.1, limit)
        self.interval = max(1, interval)
        self.unit = unit

    def _time(self) -> int:
        return int(time.time() / self.unit)

    def _key(self, slot: int) -> str:
        return f"rate/{self.resource}/{slot}"

    def _keys(self):
        base = self._time()
        for slot in range(base, base + self.interval):
            yield self._key(slot)

    def update(self, amount: int = 1) -> int:
        """Increment the cached counts for rate tracking.

        Returns:
            The updated count for the current time slot.
        """
        result = 0
        for key in self._keys():
            current = self.store.get(key) or 0
            new_value = int(current) + amount
            self.store.put(key, new_value)
            if result == 0:
                result = new_value
        return result

    def get(self, slot: int | None = None) -> int:
        """Get the current count for a time slot."""
        key = self._key(slot or self._time())
        value = self.store.get(key)
        if value is None:
            return 0
        return int(value)

    def check(self) -> bool:
        """Check if the resource is within the rate limit."""
        return self.get() < self.limit

    def comply(self, amount: int = 1) -> None:
        """Update, then sleep for the time required to adhere to the
        rate limit."""
        count = self.get()
        if count != 0:
            expected_interval = (self.interval * self.unit) / self.limit
            avg_interval = (self.interval * self.unit) / (count + 1)
            if (expected_interval - avg_interval) >= 0:
                time.sleep(expected_interval)
        self.update(amount=amount)
