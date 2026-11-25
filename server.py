"""服务入口别名：``python3 server.py`` 等价于 ``python3 coordinator.py``。

需要 HTTP 模式时：``python3 server.py --http 8765``。
"""

from coordinator import (  # noqa: F401
    Coordinator,
    CoordinationService,
    MessageCoordinator,
    Service,
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
