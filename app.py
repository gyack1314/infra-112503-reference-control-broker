"""应用入口别名，供 ``from app import service`` / ``from app import Coordinator``。"""

from coordinator import (  # noqa: F401
    Coordinator,
    CoordinationService,
    Service,
    create_service,
    main,
)

service = create_service()

if __name__ == "__main__":
    raise SystemExit(main())
