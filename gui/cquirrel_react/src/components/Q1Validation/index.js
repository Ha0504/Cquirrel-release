import React, {Component} from 'react';
import {Alert, Card, Progress, Table, Tag} from 'antd';

export default class Q1Validation extends Component {

    static defaultProps = {
        loading: false,
        validation: null,
        message: '',
    }

    render() {
        const {validation, message} = this.props;
        const comparisonColumns = [
            {
                title: 'Group',
                dataIndex: 'group',
                width: 100,
                fixed: 'left',
            },
            {
                title: 'Metric',
                dataIndex: 'metric',
                width: 150,
            },
            {
                title: 'Actual Output',
                dataIndex: 'actual',
                width: 180,
            },
            {
                title: 'Expected',
                dataIndex: 'expected',
                width: 180,
            },
            {
                title: 'Delta',
                dataIndex: 'delta',
                width: 160,
            },
            {
                title: 'Relative Delta',
                dataIndex: 'relative_delta',
                width: 180,
            },
            {
                title: 'Reason',
                dataIndex: 'match_reason',
                width: 150,
            },
            {
                title: 'Match',
                dataIndex: 'match',
                width: 100,
                render: (value) => value ? <Tag color="green">MATCH</Tag> : <Tag color="red">MISMATCH</Tag>,
            },
        ];

        const metricColumns = [
            {
                title: 'Group',
                dataIndex: 'group',
                width: 100,
                fixed: 'left',
            },
            {
                title: 'SUM_QTY',
                dataIndex: 'SUM_QTY',
                width: 180,
            },
            {
                title: 'SUM_BASE_PRICE',
                dataIndex: 'SUM_BASE_PRICE',
                width: 200,
            },
            {
                title: 'SUM_DISC_PRICE',
                dataIndex: 'SUM_DISC_PRICE',
                width: 200,
            },
            {
                title: 'SUM_CHARGE',
                dataIndex: 'SUM_CHARGE',
                width: 200,
            },
            {
                title: 'AVG_QTY',
                dataIndex: 'AVG_QTY',
                width: 180,
            },
            {
                title: 'AVG_PRICE',
                dataIndex: 'AVG_PRICE',
                width: 180,
            },
            {
                title: 'AVG_DISC',
                dataIndex: 'AVG_DISC',
                width: 180,
            },
            {
                title: 'COUNT_ORDER',
                dataIndex: 'COUNT_ORDER',
                width: 140,
            },
        ];

        const title = 'Q1 Validation';
        const summary = validation?.summary || {};
        const passingRatePercent = summary.passing_rate_percent || validation?.passing_rate_percent || '0.00';
        const passingRateNumber = Number(passingRatePercent);
        const progressPercent = Number.isFinite(passingRateNumber) ? passingRateNumber : 0;

        return (
            <Card title={title}>
                {validation && (
                    <Alert
                        type="info"
                        showIcon
                        message={`Passing Rate: ${passingRatePercent}%`}
                        description={
                            <>
                                <Progress
                                    percent={progressPercent}
                                    size="small"
                                    status="normal"
                                    style={{maxWidth: 420, marginBottom: 8}}
                                />
                                <div>
                                    {`Scale factor ${validation.scale_factor}, stream type ${validation.streams_types}, abs tolerance ${validation.abs_tolerance}, relative tolerance ${validation.rel_tolerance}. ${validation.note || ''}`}
                                </div>
                            </>
                        }
                        style={{marginBottom: 16}}
                    />
                )}

                {!validation && message && (
                    <Alert
                        type="info"
                        showIcon
                        message={message}
                        style={{marginBottom: 16}}
                    />
                )}

                {validation && (
                    <>
                        <div style={{marginBottom: 12, fontWeight: 600}}>Actual Output</div>
                        <Table
                            size="small"
                            bordered
                            pagination={false}
                            columns={metricColumns}
                            dataSource={validation.actual_rows || []}
                            scroll={{x: 1600}}
                            style={{marginBottom: 16}}
                        />

                        <div style={{marginBottom: 12, fontWeight: 600}}>Expected Reference</div>
                        <Table
                            size="small"
                            bordered
                            pagination={false}
                            columns={metricColumns}
                            dataSource={validation.expected_rows || []}
                            scroll={{x: 1600}}
                            style={{marginBottom: 16}}
                        />

                        <div style={{marginBottom: 12, fontWeight: 600}}>Metric-by-Metric Comparison</div>
                        <Table
                            size="small"
                            bordered
                            pagination={{pageSize: 16}}
                            columns={comparisonColumns}
                            dataSource={validation.comparison_rows || []}
                            scroll={{x: 1230}}
                        />
                    </>
                )}
            </Card>
        );
    }
}
