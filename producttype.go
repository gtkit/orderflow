package orderflow

// ProductType 表示订单关联商品的类型。
//
// 数值与下游业务参考标准对齐：
//
//	0  未指定（零值）
//	1  文本
//	2  视频课程
//	3  音频专栏
//	99 会员 / VIP
//
// orderflow Engine 不解读 ProductType 语义，由业务侧 OnPaid 钩子按类型派发履约逻辑。
type ProductType int8

const (
	// ProductTypeText 文本类商品。
	ProductTypeText ProductType = 1
	// ProductTypeCourse 视频课程类商品。
	ProductTypeCourse ProductType = 2
	// ProductTypeColumn 音频专栏类商品。
	ProductTypeColumn ProductType = 3
	// ProductTypeMembership 会员 / VIP 类商品。
	ProductTypeMembership ProductType = 99
)

// String 返回商品类型的中文名称，供日志 / 调试使用。
func (p ProductType) String() string {
	switch p {
	case 0:
		return "未指定"
	case ProductTypeText:
		return "文本"
	case ProductTypeCourse:
		return "视频"
	case ProductTypeColumn:
		return "音频"
	case ProductTypeMembership:
		return "会员"
	default:
		return "未知"
	}
}
