"""服务入口的常用命名别名。

核心实现位于 :mod:`coordinator`；本模块只是以 ``service`` 为名重新导出，
方便 ``from service import Service, Coordinator`` 这样的调用方式。
"""

from coordinator import (  # noqa: F401
    Coordinator,
    CoordinationService,
    LiveCoordinationService,
    MessageCoordinator,
    Service,
    create_service,
    handle_message,
    main,
    process_message,
    process_messages,
    run_file,
    run_jsonl_text,
)

if __name__ == "__main__":
    raise SystemExit(main())
