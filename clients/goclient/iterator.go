package goclient

// Next retrieves the next query result from the stream.
// It returns io.EOF when the stream is finished.
func (it *QueryResultIterator) Next() (*QueryResultItem, error) {
	res, err := it.stream.Recv()
	if err != nil {
		// io.EOF เป็นสัญญาณบอกว่า stream สิ้นสุดแล้ว ซึ่งผู้เรียกต้องจัดการเอง
		return nil, err
	}

	// สมมติว่าโครงสร้างของ pb.QueryResult สอดคล้องกับ QueryResultItem
	// การเรียก .AsMap() อาจล้มเหลวหากค่าใน struct ไม่ใช่ประเภทที่รองรับ
	// แต่ในตัวอย่างนี้จะข้ามการจัดการ error ไปก่อน โค้ดจริงควรจัดการให้รัดกุม
	fields := make(map[string]interface{})
	if res.GetFields() != nil {
		fields = res.GetFields().AsMap()
	}

	item := &QueryResultItem{
		Metric:           res.GetMetric(),
		Tags:             res.GetTags(),
		Timestamp:        res.GetTimestamp(),
		Fields:           fields,
		IsAggregated:     res.GetIsAggregated(),
		WindowStartTime:  res.GetWindowStartTime(),
		WindowEndTime:    res.GetWindowEndTime(),
		AggregatedValues: res.GetAggregatedValues(),
	}

	return item, nil
}
