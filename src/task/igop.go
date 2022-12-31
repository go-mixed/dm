package task

import (
	"github.com/goplus/igop"
	"github.com/pkg/errors"
	"gopkg.in/go-mixed/igop.v1/mod"
)

func buildIgop(path string, debug bool) (*mod.Context, error) {
	ctx := mod.NewContext(path, debug)

	if err := ctx.LoadVendor(""); err != nil {
		return nil, errors.Wrapf(err, "[igop]load vendor of \"%s\" error", path)
	}

	if err := ctx.Build(); err != nil {
		return nil, errors.Wrapf(err, "[igop]build \"%s\" error", path)
	}

	return ctx, nil
}

// 每一次运行都是全新的会话，即上一个运行的变量结果无法被下一个调用
func igopCall(ctx *mod.Context, method string, args []igop.Value) (igop.Value, error) {
	interp, err := ctx.NewInterp(ctx.GetMainPackage())
	if err != nil {
		return nil, errors.Wrap(err, "[igop]make Interp error")
	}

	if ctx.GetMainPackage().Func("init") != nil {
		if err = interp.RunInit(); err != nil {
			return nil, errors.Wrap(err, "[igop]run \"init\" error")
		}
	}

	v, err := interp.RunFunc(method, args...)
	if err != nil {
		return v, errors.Wrapf(err, "[igop]run \"%s\" error", method)
	}
	return v, nil
}
