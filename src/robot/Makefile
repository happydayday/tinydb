
#
#
# BIN				- 可执行文件
# LIBA				- 静态库
# LIBSO				- 动态库
#
# VERSION			- 版本号
#
# DEPEND_LIBS		- 依赖的其他第三方库
# DEPEND_MODULES	- 依赖的项目的其他模块
#
#

BIN				= roboted

PRODUCT 		= TinyDBRobot
VERSION			= 3.0.3

DEPEND_LIBS		= evlite pthread

DEPEND_MODULES 	= $(ROOT)/src/io \
					$(ROOT)/src/utils \
					$(ROOT)/src/message

include $(ROOT)/Makefile.rules
