from ..services.expression_parser import OperationEnum

ADD_TASKS_TOPIC = "add_tasks"
SUB_TASKS_TOPIC = "sub_tasks"
MUL_TASKS_TOPIC = "mul_tasks"
DIV_TASKS_TOPIC = "div_tasks"

ADD_TASKS_WRAPPER_TOPIC = "add_tasks_wrapper"
SUB_TASKS_WRAPPER_TOPIC = "sub_tasks_wrapper"
MUL_TASKS_WRAPPER_TOPIC = "mul_tasks_wrapper"
DIV_TASKS_WRAPPER_TOPIC = "div_tasks_wrapper"

XSUM_TASKS_TOPIC = "xsum_tasks"
XPROD_TASKS_TOPIC = "xprod_tasks"

OPERATION_TOPIC_MAP = {
    OperationEnum.ADD: ADD_TASKS_TOPIC,
    OperationEnum.SUB: SUB_TASKS_TOPIC,
    OperationEnum.MUL: MUL_TASKS_TOPIC,
    OperationEnum.DIV: DIV_TASKS_TOPIC,
}

OPERATION_WRAPPER_TOPIC_MAP = {
    OperationEnum.ADD: ADD_TASKS_WRAPPER_TOPIC,
    OperationEnum.SUB: SUB_TASKS_WRAPPER_TOPIC,
    OperationEnum.MUL: MUL_TASKS_WRAPPER_TOPIC,
    OperationEnum.DIV: DIV_TASKS_WRAPPER_TOPIC,
}

AGGREGATOR_TOPIC_MAP = {
    OperationEnum.ADD: XSUM_TASKS_TOPIC,
    OperationEnum.MUL: XPROD_TASKS_TOPIC,
}