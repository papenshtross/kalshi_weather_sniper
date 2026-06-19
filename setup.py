from setuptools import Extension, setup

setup(
    ext_modules=[
        Extension(
            "polybot.live._nwws_fast",
            sources=["polybot/live/_nwws_fast.c"],
            extra_compile_args=["-O3", "-march=native"],
        )
    ]
)
